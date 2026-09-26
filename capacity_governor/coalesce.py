"""Short-lived coalescing (single-flight) for identical read-only requests.

Semantics:

* Concurrent calls with the same key share one execution. The result is only
  delivered once the leader's work has actually finished — never early.
* Successful results are cached for ``ttl_s`` seconds; hits are served from
  cache without re-executing.
* If the leader fails or is cancelled, nothing is cached and every waiter
  observes the failure — subsequent requests re-execute (no pollution).
* ``invalidate(key)`` aborts an in-flight execution: waiters get
  ``AdmissionRejected(CONCURRENCY_CONFLICT)`` so they can retry under new
  rules, and the stale execution is cancelled so its result cannot be cached.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from .errors import AdmissionRejected, RejectReason


class _Flight:
    """One in-flight execution shared by any number of waiters."""

    __slots__ = ("future", "task")

    def __init__(self) -> None:
        self.future: asyncio.Future = asyncio.get_running_loop().create_future()
        self.task: asyncio.Task | None = None


class Coalescer:
    """Merges concurrent identical calls; caches successes for a short TTL."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._inflight: dict[str, _Flight] = {}
        self._cache: dict[str, tuple[float, Any]] = {}

    async def run(
        self,
        key: str,
        ttl_s: float,
        factory: Callable[[], Awaitable[Any]],
    ) -> tuple[Any, bool]:
        """Return ``(result, shared)``; ``shared`` means the work was reused."""
        async with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                expires_at, value = cached
                if time.monotonic() < expires_at:
                    return value, True
                del self._cache[key]  # expired: must not be served

            flight = self._inflight.get(key)
            if flight is None:
                flight = _Flight()
                # Assign the task while still holding the lock so invalidate()
                # can never observe a flight without its task.
                flight.task = asyncio.create_task(
                    self._lead(key, ttl_s, flight, factory)
                )
                self._inflight[key] = flight
                shared = False
            else:
                shared = True

        return await self._join(flight, shared)

    async def _lead(
        self,
        key: str,
        ttl_s: float,
        flight: _Flight,
        factory: Callable[[], Awaitable[Any]],
    ) -> None:
        try:
            value = await factory()
        except BaseException as exc:
            # Failure/cancel: publish nothing; waiters see the same failure.
            if not flight.future.done():
                flight.future.set_exception(exc)
        else:
            # If invalidate() already finished the future, this result is
            # stale: do not cache it and do not deliver it.
            if not flight.future.done():
                if ttl_s > 0:
                    self._cache[key] = (time.monotonic() + ttl_s, value)
                flight.future.set_result(value)
        finally:
            async with self._lock:
                if self._inflight.get(key) is flight:
                    del self._inflight[key]

    async def _join(self, flight: _Flight, shared: bool) -> tuple[Any, bool]:
        try:
            value = await asyncio.shield(flight.future)
        except asyncio.CancelledError:
            # Our own waiter task was cancelled; the flight itself continues
            # for the other waiters (shielded). Propagate the cancellation.
            raise
        return value, shared

    async def invalidate(self, key: str) -> None:
        """Drop cached value and abort any in-flight execution for ``key``."""
        async with self._lock:
            self._cache.pop(key, None)
            flight = self._inflight.pop(key, None)
        if flight is not None:
            conflict = AdmissionRejected(
                RejectReason.CONCURRENCY_CONFLICT,
                f"{key}: in-flight execution invalidated",
            )
            if not flight.future.done():
                flight.future.set_exception(conflict)
            if flight.task is not None:
                flight.task.cancel()
