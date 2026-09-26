"""Dynamic configuration model for the capacity governor."""

from __future__ import annotations

from dataclasses import dataclass, field, replace


@dataclass(frozen=True, slots=True)
class RouteLimit:
    """Capacity limits for one (route, caller) dimension."""

    max_concurrent: int = 4
    max_queued: int = 8
    queue_timeout_s: float = 2.0
    coalesce_enabled: bool = True
    coalesce_ttl_s: float = 0.5


@dataclass(frozen=True, slots=True)
class GovernorConfig:
    """Whole-governor config. Treated as immutable; updates swap the object."""

    default_limit: RouteLimit = field(default_factory=RouteLimit)
    route_limits: dict[str, RouteLimit] = field(default_factory=dict)
    version: int = 0

    def limit_for(self, route: str) -> RouteLimit:
        """Return the effective limit for a route (fall back to default)."""
        return self.route_limits.get(route, self.default_limit)

    def with_updates(
        self,
        *,
        default_limit: RouteLimit | None = None,
        route_limits: dict[str, RouteLimit] | None = None,
    ) -> "GovernorConfig":
        """Return a new config with bumped version (no in-place mutation)."""
        return replace(
            self,
            default_limit=default_limit if default_limit is not None else self.default_limit,
            route_limits=route_limits if route_limits is not None else self.route_limits,
            version=self.version + 1,
        )
