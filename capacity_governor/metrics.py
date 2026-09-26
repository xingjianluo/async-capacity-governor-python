"""Runtime metrics: latency split, rejection reasons, execution counts."""

from __future__ import annotations

import threading
from collections import defaultdict


class MetricsRegistry:
    """Thread-safe counters and latency observations for the governor."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = defaultdict(int)
        self._latency_sum: dict[str, float] = defaultdict(float)
        self._latency_count: dict[str, int] = defaultdict(int)

    def incr(self, name: str, value: int = 1) -> None:
        """Increment a counter (e.g. rejected.queue_full)."""
        with self._lock:
            self._counters[name] += value

    def observe_latency(self, name: str, seconds: float) -> None:
        """Record a latency observation (queue or execute phase)."""
        with self._lock:
            self._latency_sum[name] += seconds
            self._latency_count[name] += 1

    def snapshot(self) -> dict:
        """Return a consistent snapshot of counters and latency stats."""
        with self._lock:
            latencies = {
                name: {
                    "count": self._latency_count[name],
                    "total_s": round(self._latency_sum[name], 6),
                    "avg_s": round(self._latency_sum[name] / self._latency_count[name], 6),
                }
                for name in self._latency_count
            }
            return {"counters": dict(self._counters), "latencies": latencies}
