"""Per-dimension admission control with bounded queueing."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from .config import RouteLimit
from .errors import RejectReason
from .metrics import MetricsRegistry


@dataclass(slots=True)
class Permit:
    """An acquired admission slot; released exactly once."""

    dimension: str
    queue_wait_s: float
    config_version: int
    _lane: "AdmissionLane"
    _released: bool = False

    def release(self) -> None:
        """Release the slot back to the lane (idempotent)."""
        if not self._released:
            self._released = True
            self._lane._release(self)


class AdmissionLane:
    """One (route, caller) lane: bounded concurrency + bounded FIFO queue."""

    def __init__(self, dimension: str, metrics: MetricsRegistry) -> None:
        self.dimension = dimension
        self._metrics = metrics
        self._cond = asyncio.Condition()
        self._running = 0
        self._waiting = 0

    async def acquire(self, limit: RouteLimit, config_version: int) -> Permit:
        """Acquire a slot, queueing up to limit.max_queued / queue_timeout_s."""
        raise NotImplementedError

    def _release(self, permit: Permit) -> None:
        """Return a slot and wake one waiter."""
        raise NotImplementedError

    def stats(self) -> dict:
        """Current running/waiting counts for this lane."""
        return {"running": self._running, "waiting": self._waiting}
