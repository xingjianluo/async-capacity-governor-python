"""CapacityGovernor: ties admission, coalescing, metrics and config together."""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .admission import AdmissionLane
from .coalesce import Coalescer
from .config import GovernorConfig
from .metrics import MetricsRegistry


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
    """Coordinates admission lanes, coalescing and dynamic config."""

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
        raise NotImplementedError

    async def execute(
        self,
        route: str,
        caller: str,
        handler: Callable[[], Awaitable[Any]],
        *,
        coalesce_key: str | None = None,
        degraded: Callable[[], Awaitable[Any]] | None = None,
    ) -> Outcome:
        """Run handler under admission control, optional coalescing/degradation."""
        raise NotImplementedError
