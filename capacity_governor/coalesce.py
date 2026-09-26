"""Short-lived single-flight coalescing for identical read-only requests."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any


class Coalescer:
    """Merges concurrent identical calls; caches results for a short TTL."""

    def __init__(self) -> None:
        self._inflight: dict[str, asyncio.Task] = {}
        self._cache: dict[str, tuple[float, Any]] = {}

    async def run(
        self,
        key: str,
        ttl_s: float,
        factory: Callable[[], Awaitable[Any]],
    ) -> tuple[Any, bool]:
        """Return (result, shared). `shared` is True if this call reused work."""
        raise NotImplementedError

    def invalidate(self, key: str) -> None:
        """Drop a cached entry (e.g. after TTL or explicit invalidation)."""
        self._cache.pop(key, None)
