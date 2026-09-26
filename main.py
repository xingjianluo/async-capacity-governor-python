"""HTTP surface for the async capacity governor.

Endpoints:
    GET  /items/{item_id}      read-only, coalescible, admission-controlled
    GET  /metrics              counters, latency split, per-lane stats
    GET  /config               current config (with version)
    PUT  /config               swap config (in-flight requests keep old snapshot)
    POST /admin/invalidate/{k} abort an in-flight coalesced execution

Outcome -> HTTP mapping (success/degraded/rejected/cancelled are distinct):
    success   -> 200, body.status = "success"
    degraded  -> 200, body.status = "degraded" (fallback payload)
    rejected  -> 429 queue_full / 503 queue_timeout / 409 concurrency_conflict
    cancelled -> 499 (client cancelled while queued or executing)
"""

from __future__ import annotations

import asyncio
import uuid

import uvicorn
from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, Field

from capacity_governor import (
    CapacityGovernor,
    GovernorConfig,
    OutcomeKind,
    RouteLimit,
)

REJECT_STATUS = {
    "queue_full": 429,
    "queue_timeout": 503,
    "concurrency_conflict": 409,
}
CANCELLED_STATUS = 499


class RouteLimitModel(BaseModel):
    max_concurrent: int = Field(default=4, ge=0)
    max_queued: int = Field(default=8, ge=0)
    queue_timeout_s: float = Field(default=2.0, ge=0.0)
    coalesce_enabled: bool = True
    coalesce_ttl_s: float = Field(default=0.5, ge=0.0)

    def to_limit(self) -> RouteLimit:
        return RouteLimit(**self.model_dump())


class ConfigModel(BaseModel):
    default_limit: RouteLimitModel = Field(default_factory=RouteLimitModel)
    route_limits: dict[str, RouteLimitModel] = Field(default_factory=dict)


def create_app(governor: CapacityGovernor | None = None) -> FastAPI:
    gov = governor or CapacityGovernor()
    app = FastAPI(title="async-capacity-governor")
    app.state.governor = gov

    @app.get("/")
    def read_root() -> dict:
        return {"Hello": "World"}

    @app.get("/items/{item_id}")
    async def get_item(
        request: Request,
        item_id: int,
        delay: float = 0.05,
        fail: bool = False,
        degrade: bool = True,
    ) -> Response:
        caller = request.headers.get("x-caller", "anonymous")
        request_id = request.headers.get("x-request-id", uuid.uuid4().hex[:12])

        async def work() -> dict:
            await asyncio.sleep(delay)  # simulated downstream read
            if fail:
                raise RuntimeError(f"item {item_id}: downstream failure")
            return {"item_id": item_id, "payload": f"item-{item_id}"}

        async def fallback() -> dict:
            return {"item_id": item_id, "payload": "stale-fallback"}

        outcome = await gov.execute(
            "/items/{item_id}",
            caller,
            work,
            request_id=request_id,
            coalesce_key=f"item:{item_id}",
            degraded=fallback if degrade else None,
        )
        return _respond(outcome)

    @app.get("/metrics")
    async def metrics() -> dict:
        snap = gov.metrics.snapshot()
        snap["lanes"] = gov.lane_stats()
        snap["config_version"] = gov.config.version
        return snap

    @app.get("/config")
    async def get_config() -> dict:
        cfg = gov.config
        return {
            "version": cfg.version,
            "default_limit": cfg.default_limit.__dict__
            if hasattr(cfg.default_limit, "__dict__")
            else {f: getattr(cfg.default_limit, f) for f in RouteLimit.__dataclass_fields__},
            "route_limits": {
                r: {f: getattr(l, f) for f in RouteLimit.__dataclass_fields__}
                for r, l in cfg.route_limits.items()
            },
        }

    @app.put("/config")
    async def put_config(body: ConfigModel) -> dict:
        new_cfg = gov.config.with_updates(
            default_limit=body.default_limit.to_limit(),
            route_limits={r: m.to_limit() for r, m in body.route_limits.items()},
        )
        gov.update_config(new_cfg)
        return {"version": new_cfg.version}

    @app.post("/admin/invalidate/{key}")
    async def invalidate(key: str) -> dict:
        await gov.invalidate(key)
        return {"invalidated": key}

    return app


def _respond(outcome) -> Response:
    from fastapi.responses import JSONResponse

    body = {
        "status": outcome.kind.value,
        "value": outcome.value,
        "reason": outcome.reason,
        "shared": outcome.shared,
        "queue_wait_s": round(outcome.queue_wait_s, 6),
        "execute_s": round(outcome.execute_s, 6),
        "config_version": outcome.config_version,
    }
    if outcome.kind is OutcomeKind.SUCCESS:
        status = 200
    elif outcome.kind is OutcomeKind.DEGRADED:
        status = 200
    elif outcome.kind is OutcomeKind.CANCELLED:
        status = CANCELLED_STATUS
    else:
        status = REJECT_STATUS.get(outcome.reason or "", 503)
    return JSONResponse(body, status_code=status)


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
