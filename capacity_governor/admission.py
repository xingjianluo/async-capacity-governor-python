"""Per-dimension admission control with bounded concurrency and a bounded FIFO queue.

Each (route, caller) pair maps to one :class:`AdmissionLane`. A request that
cannot get a slot immediately joins the lane's queue. The queue is bounded:

* ``running >= max_concurrent`` and ``waiting >= max_queued`` -> reject
  immediately with ``QUEUE_FULL`` (no unbounded memory growth).
* Waiting longer than ``queue_timeout_s`` -> ``QUEUE_TIMEOUT``.
* The waiter's task being cancelled while queued -> ``CancelledError``
  (mapped by the governor to a CANCELLED outcome).

Limits are passed in as a per-request snapshot taken at admission time, so a
config swap never mutates the semantics a request was admitted under.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from .config import RouteLimit
from .metrics import MetricsRegistry


@dataclass(slots=True)
class Permit:
    """An acquired admission slot; released back to its lane exactly once."""

    dimension: str
    queue_wait_s: float
    config_version: int
    _lane: "AdmissionLane | None" = field(default=None, repr=False)
    _released: bool = False

    async def release(self) -> None:
        """Return the slot and wake one waiter. Idempotent."""
        if self._released or self._lane is None:
            return
        self._released = True
        await self._lane._release()


class AdmissionLane:
    """One bounded-concurrency / bounded-queue lane."""

    def __init__(self, dimension: str, metrics: MetricsRegistry) -> None:
        self.dimension = dimension
        self._metrics = metrics
        self._cond = asyncio.Condition()
        self._running = 0
        self._waiting = 0

    async def acquire(
        self,
        limit: RouteLimit,
        config_version: int,
        *,
        cancelled_factory: object = None,
    ) -> Permit:
        """Acquire a slot, queueing under the given (immutable) limit snapshot.

        Raises:
            AdmissionRejected(QUEUE_FULL): queue is already full.
            QueueTimeout: the queue timeout elapsed before a slot freed.
            asyncio.CancelledError: the waiting task was cancelled.
        """
        loop = asyncio.get_running_loop()
        started = loop.time()
        async with self._cond:
            if self._running < limit.max_concurrent:
                self._running += 1
                return Permit(self.dimension, 0.0, config_version, self)

            if self._waiting >= limit.max_queued:
                self._metrics.incr("rejected.queue_full")
                from .errors import AdmissionRejected, RejectReason

                raise AdmissionRejected(
                    RejectReason.QUEUE_FULL,
                    f"{self.dimension}: {self._running} running, "
                    f"{self._waiting} queued",
                )

            self._waiting += 1
            try:
                while self._running >= limit.max_concurrent:
                    remaining = limit.queue_timeout_s - (loop.time() - started)
                    if remaining <= 0:
                        from .errors import QueueTimeout

                        raise QueueTimeout(
                            f"{self.dimension}: waited > "
                            f"{limit.queue_timeout_s:.3f}s",
                        )
                    try:
                        await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                    except TimeoutError:
                        from .errors import QueueTimeout

                        raise QueueTimeout(
                            f"{self.dimension}: waited > "
                            f"{limit.queue_timeout_s:.3f}s",
                        )
                # Woken: slot is ours under the snapshot we were queued with.
            except BaseException:
                # Timeout, cancel or rejection: give up our queue position and
                # hand the wakeup to the next waiter so counts never drift.
                self._waiting -= 1
                self._cond.notify()
                raise

            self._waiting -= 1
            self._running += 1
            return Permit(
                self.dimension, loop.time() - started, config_version, self
            )

    async def _release(self) -> None:
        async with self._cond:
            self._running -= 1
            self._cond.notify()

    @property
    def running(self) -> int:
        return self._running

    @property
    def waiting(self) -> int:
        return self._waiting

    def stats(self) -> dict:
        return {"running": self._running, "waiting": self._waiting}
