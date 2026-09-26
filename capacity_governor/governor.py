"""CapacityGovernor: ties admission, coalescing, metrics and dynamic config.

Request lifecycle inside :meth:`CapacityGovernor.execute`:

1. Snapshot the current config (in-flight requests keep this snapshot, so a
   config swap never mutates their semantics or double-executes them).
2. Acquire an admission permit for the ``(route, caller)`` lane.
   * ``QUEUE_FULL`` / ``QUEUE_TIMEOUT`` -> degraded handler if given, else a
     REJECTED outcome. Never waits unboundedly.
   * client cancellation while queued -> CANCELLED outcome.
3. Run the handler, optionally through the coalescer (identical read-only
   requests share one execution; only the leader actually executes).
4. Record queue/execute latency split and per-reason counters, log the
   request id with its admission decision and timing breakdown.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .admission import AdmissionLane, Permit
from .coalesce import Coalescer
from .config import GovernorConfig
from .errors import AdmissionRejected, QueueTimeout, RejectReason
from .metrics import MetricsRegistry

logger = logging.getLogger("capacity_governor")


class OutcomeKind(enum.Enum):
    """How a request concluded."""

    SUCCESS = "success"
    DEGRADED = "degraded"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


@dataclass(slots=True)
class Outcome:
    """Result envelope returned by the governor."""

    kind: OutcomeKind
    value: Any = None
    reason: str | None = None
    shared: bool = False
    queue_wait_s: float = 0.0
    execute_s: float = 0.0
    config_version: int = 0


class CapacityGovernor:
    """Coordinates admission lanes, coalescing, metrics and dynamic config."""

    def __init__(self, config: GovernorConfig | None = None) -> None:
        self._config = config or GovernorConfig()
        self._metrics = MetricsRegistry()
        self._lanes: dict[str, AdmissionLane] = {}
        self._coalescer = Coalescer()

    @property
    def metrics(self) -> MetricsRegistry:
        return self._metrics

    @property
    def config(self) -> GovernorConfig:
        return self._config

    def update_config(self, config: GovernorConfig) -> None:
        """Swap config atomically; in-flight requests keep their snapshot."""
        self._config = config
        logger.info("config updated to version %d", config.version)

    async def invalidate(self, coalesce_key: str) -> None:
        """Abort an in-flight coalesced execution (waiters get a conflict)."""
        await self._coalescer.invalidate(coalesce_key)

    def lane_stats(self) -> dict[str, dict]:
        return {name: lane.stats() for name, lane in self._lanes.items()}

    def _lane(self, dimension: str) -> AdmissionLane:
        lane = self._lanes.get(dimension)
        if lane is None:
            lane = self._lanes[dimension] = AdmissionLane(dimension, self._metrics)
        return lane

    async def execute(
        self,
        route: str,
        caller: str,
        handler: Callable[[], Awaitable[Any]],
        *,
        request_id: str = "-",
        coalesce_key: str | None = None,
        degraded: Callable[[], Awaitable[Any]] | None = None,
    ) -> Outcome:
        """Run ``handler`` under admission control, coalescing and degradation."""
        cfg = self._config  # snapshot: stable for the whole request
        limit = cfg.limit_for(route)
        dimension = f"{route}:{caller}"

        try:
            permit = await self._lane(dimension).acquire(limit, cfg.version)
        except QueueTimeout as exc:
            self._metrics.incr("rejected.queue_timeout")
            return await self._refuse(
                request_id, dimension, RejectReason.QUEUE_TIMEOUT, str(exc), degraded
            )
        except AdmissionRejected as exc:
            return await self._refuse(
                request_id, dimension, exc.reason, str(exc), degraded
            )
        except asyncio.CancelledError:
            self._metrics.incr("cancelled")
            logger.info(
                "req=%s dim=%s decision=cancelled-while-queued", request_id, dimension
            )
            return Outcome(
                OutcomeKind.CANCELLED,
                reason=RejectReason.CANCELLED.value,
                config_version=cfg.version,
            )

        self._metrics.incr("admitted")
        self._metrics.observe_latency("queue_wait", permit.queue_wait_s)
        logger.info(
            "req=%s dim=%s decision=admitted queue_wait=%.4fs cfg=%d",
            request_id,
            dimension,
            permit.queue_wait_s,
            cfg.version,
        )

        try:
            return await self._run(
                request_id, dimension, limit, handler, coalesce_key, permit
            )
        finally:
            await permit.release()

    async def _run(
        self,
        request_id: str,
        dimension: str,
        limit,
        handler,
        coalesce_key: str | None,
        permit: Permit,
    ) -> Outcome:
        started = time.perf_counter()
        shared = False
        try:
            if coalesce_key is not None and limit.coalesce_enabled:
                value, shared = await self._coalescer.run(
                    coalesce_key, limit.coalesce_ttl_s, handler
                )
            else:
                value = await handler()
        except asyncio.CancelledError:
            self._metrics.incr("cancelled")
            logger.info(
                "req=%s dim=%s decision=cancelled-while-executing",
                request_id,
                dimension,
            )
            return Outcome(
                OutcomeKind.CANCELLED,
                reason=RejectReason.CANCELLED.value,
                queue_wait_s=permit.queue_wait_s,
                config_version=permit.config_version,
            )
        except AdmissionRejected as exc:
            # Coalescing conflict (in-flight execution invalidated).
            self._metrics.incr(f"rejected.{exc.reason.value}")
            logger.info(
                "req=%s dim=%s decision=conflict reason=%s",
                request_id,
                dimension,
                exc.reason.value,
            )
            return Outcome(
                OutcomeKind.REJECTED,
                reason=exc.reason.value,
                queue_wait_s=permit.queue_wait_s,
                config_version=permit.config_version,
            )
        except Exception:
            self._metrics.incr("errors")
            raise

        execute_s = time.perf_counter() - started
        self._metrics.observe_latency("execute", execute_s)
        if shared:
            self._metrics.incr("coalesced.shared")
        else:
            self._metrics.incr("executed")
        logger.info(
            "req=%s dim=%s decision=completed shared=%s "
            "queue_wait=%.4fs execute=%.4fs cfg=%d",
            request_id,
            dimension,
            shared,
            permit.queue_wait_s,
            execute_s,
            permit.config_version,
        )
        return Outcome(
            OutcomeKind.SUCCESS,
            value=value,
            shared=shared,
            queue_wait_s=permit.queue_wait_s,
            execute_s=execute_s,
            config_version=permit.config_version,
        )

    async def _refuse(
        self,
        request_id: str,
        dimension: str,
        reason: RejectReason,
        detail: str,
        degraded: Callable[[], Awaitable[Any]] | None,
    ) -> Outcome:
        logger.info(
            "req=%s dim=%s decision=%s detail=%s",
            request_id,
            dimension,
            reason.value,
            detail,
        )
        if degraded is not None:
            started = time.perf_counter()
            value = await degraded()
            execute_s = time.perf_counter() - started
            self._metrics.incr("degraded")
            self._metrics.observe_latency("execute", execute_s)
            logger.info(
                "req=%s dim=%s decision=degraded reason=%s execute=%.4fs",
                request_id,
                dimension,
                reason.value,
                execute_s,
            )
            return Outcome(
                OutcomeKind.DEGRADED, value=value, reason=reason.value,
                execute_s=execute_s,
            )
        return Outcome(OutcomeKind.REJECTED, reason=reason.value)
