"""FastAPI 装配：演示接口、管理接口与异常 -> HTTP 响应映射。

响应约定（成功 / 拒绝 / 降级可区分）：
- 200 success/success_cache/success_coalesced：data 为业务结果；
- 200 degraded：data 为兜底值，degraded=true 并带 degrade_reason；
- 429 capacity_rejected：并发与队列均满（未配置降级）；
- 503 queue_timeout：排队超时（未配置降级）；
- 499 client_canceled：客户端取消（通常只出现在服务端日志）；
- 500 execution_failed：业务执行失败且未配置降级。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from .config import ConfigStore, Policy, Snapshot
from .errors import GovernorError
from .governor import (
    STATUS_CACHE,
    STATUS_COALESCED,
    STATUS_DEGRADED,
    Governor,
)
from .metrics import MetricsRegistry

logger = logging.getLogger("governor")

DEFAULT_SNAPSHOT = Snapshot(
    default=Policy(
        max_concurrency=4,
        max_queue=4,
        queue_timeout=0.5,
        coalesce_window=0.0,
        result_ttl=1.0,
        fallback_value=None,
    ),
    policies={
        # 热点只读接口：开启合并 + 1 秒结果复用
        ("items", "*"): Policy(
            max_concurrency=2,
            max_queue=2,
            queue_timeout=0.3,
            coalesce_window=1.0,
            result_ttl=1.0,
            fallback_value=None,
        ),
        # 演示降级：容量受限时返回兜底值
        ("report", "*"): Policy(
            max_concurrency=1,
            max_queue=1,
            queue_timeout=0.2,
            coalesce_window=0.0,
            result_ttl=0.0,
            fallback_value={"degraded": True, "items": []},
        ),
    },
)

# 各演示接口的“后端执行”函数
async def _work_read(key: str, delay: float) -> dict[str, object]:
    await asyncio.sleep(delay)
    return {"key": key, "computed": True}


async def _work_report(delay: float, fail: bool) -> dict[str, object]:
    await asyncio.sleep(delay)
    if fail:
        raise RuntimeError("backend failure")
    return {"report": "ok", "delay": delay}


def _timing(outcome: Any) -> dict[str, object]:
    return {
        "queue_wait_ms": round(outcome.queue_wait * 1000, 3),
        "execute_ms": round(outcome.execute_time * 1000, 3),
        "total_ms": round(outcome.total_time * 1000, 3),
    }


def create_app(store: ConfigStore | None = None) -> FastAPI:
    store = store or ConfigStore(DEFAULT_SNAPSHOT)
    governor = Governor(store, metrics=MetricsRegistry())
    governor.reload()
    app = FastAPI(title="async-capacity-governor", version="0.1.0")
    app.state.config_store = store
    app.state.governor = governor

    @app.middleware("http")
    async def add_request_context(request: Request, call_next: Any) -> Any:
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
        request.state.request_id = rid
        try:
            return await call_next(request)
        except GovernorError as exc:
            # 正常路径下治理异常已在端点内映射；这里兜底
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "request_id": rid,
                    "status": "rejected",
                    "error": exc.error_code,
                    "detail": str(exc),
                },
            )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    async def governed(
        request: Request,
        endpoint: str,
        caller: str,
        factory: Any,
        coalesce_key: str | None = None,
    ) -> JSONResponse:
        rid = request.state.request_id
        handle_coro = governor.handle(
            endpoint, caller, rid, factory, coalesce_key=coalesce_key
        )
        try:
            # shield：客户端断开只取消“等待响应”的外层任务，
            # 治理流程（准入释放、合并 leader 的后端执行）继续跑完，
            # 避免在途请求被连带杀执行或造成计数漂移。
            outcome = await asyncio.shield(handle_coro)
        except asyncio.CancelledError:
            # 若治理流程恰在同一时刻已完成则尽量取结果，否则按 499
            if not handle_coro.done():
                return JSONResponse(
                    status_code=499,
                    content={
                        "request_id": rid,
                        "status": "canceled",
                        "error": "client_canceled",
                    },
                )
            outcome = handle_coro.result()
        return _outcome_to_response(rid, outcome)

    @app.get("/items/{key}")
    async def get_item(
        request: Request,
        key: str,
        caller: str = Query("anonymous"),
        delay: float = Query(0.1, ge=0),
    ) -> JSONResponse:
        rid = request.state.request_id
        return await governed(
            request,
            "items",
            caller,
            lambda: _work_read(key, delay),
            coalesce_key=f"items:{key}",
        )

    @app.get("/report")
    async def get_report(
        request: Request,
        caller: str = Query("anonymous"),
        delay: float = Query(0.1, ge=0),
        fail: bool = Query(False),
    ) -> JSONResponse:
        rid = request.state.request_id
        return await governed(
            request,
            "report",
            caller,
            lambda: _work_report(delay, fail),
        )

    @app.get("/admin/metrics")
    async def admin_metrics() -> dict[str, object]:
        return {
            "scopes": governor.metrics.snapshot(),
            "gates": {
                f"{ep}:{caller}": {"active": a, "waiting": w}
                for (ep, caller), (a, w) in governor.limiter.states().items()
            },
            "flights": governor.coalescer.flight_count(),
        }

    @app.post("/admin/config")
    async def admin_config(request: Request) -> JSONResponse:
        payload = await request.json()
        try:
            snap = _parse_snapshot(payload)
        except (KeyError, TypeError, ValueError) as exc:
            return JSONResponse(
                status_code=400, content={"error": "invalid_config", "detail": str(exc)}
            )
        store.replace(snap)
        governor.reload()
        logger.info("config replaced at runtime: %d policies", len(snap.policies))
        return JSONResponse({"status": "applied", "policies": len(snap.policies)})

    return app


def _outcome_to_response(rid: str, outcome: Any) -> JSONResponse:
    if outcome.status == "rejected":
        exc = outcome.cause
        status_code = getattr(exc, "status_code", 429) if exc else 429
        error_code = getattr(exc, "error_code", "rejected") if exc else "rejected"
        return JSONResponse(
            status_code=status_code,
            content={
                "request_id": rid,
                "status": "rejected",
                "error": error_code,
                "reason": outcome.reject_reason,
                "detail": str(exc) if exc else "rejected",
                "timing": {
                    "queue_wait_ms": round(outcome.queue_wait * 1000, 3),
                    "total_ms": round(outcome.total_time * 1000, 3),
                },
            },
        )
    if outcome.status == "failed":
        return JSONResponse(
            status_code=500,
            content={
                "request_id": rid,
                "status": "failed",
                "error": "execution_failed",
                "detail": str(outcome.cause) if outcome.cause else "failed",
                "timing": {
                    "queue_wait_ms": round(outcome.queue_wait * 1000, 3),
                    "execute_ms": round(outcome.execute_time * 1000, 3),
                    "total_ms": round(outcome.total_time * 1000, 3),
                },
            },
        )
    if outcome.status == "canceled":
        return JSONResponse(
            status_code=499,
            content={
                "request_id": rid,
                "status": "canceled",
                "error": "client_canceled",
            },
        )
    # success / cache / coalesced / degraded
    status = (
        "degraded"
        if outcome.status == STATUS_DEGRADED
        else "coalesced"
        if outcome.status == STATUS_COALESCED
        else "cache"
        if outcome.status == STATUS_CACHE
        else "success"
    )
    body: dict[str, object] = {
        "request_id": rid,
        "status": status,
        "data": outcome.value,
        "timing": _timing(outcome),
        "executed": outcome.executed,
    }
    if status == "degraded":
        body["degraded"] = True
        body["degrade_reason"] = outcome.reject_reason or "execution_failed"
    return JSONResponse(status_code=200, content=body)


def _parse_snapshot(payload: dict[str, Any]) -> Snapshot:
    default = (
        _parse_policy(payload["default"]) if "default" in payload else Policy()
    )
    policies: dict[tuple[str, str], Policy] = {}
    for entry in payload.get("policies", []):
        policies[(entry["endpoint"], entry.get("caller", "*"))] = _parse_policy(
            entry
        )
    return Snapshot(default=default, policies=policies)


def _parse_policy(entry: dict[str, Any]) -> Policy:
    allowed = {
        "max_concurrency",
        "max_queue",
        "queue_timeout",
        "coalesce_window",
        "result_ttl",
        "fallback_value",
    }
    kwargs = {k: v for k, v in entry.items() if k in allowed}
    return Policy(**kwargs)
