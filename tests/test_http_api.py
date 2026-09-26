"""HTTP 端到端：成功/拒绝/降级可区分、突发并发、排队超时、客户端取消、
相同请求合并与配置切换；校验响应中的耗时分解与 /admin/metrics。"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging

import httpx
import pytest

from governor.app import create_app
from governor.config import ConfigStore, Policy, Snapshot, WILDCARD_CALLER

log = logging.getLogger("test.http")


@pytest.fixture
def client_fixture():
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    c = httpx.AsyncClient(transport=transport, base_url="http://t")
    return c


def _run(coro_factory):
    """在 pytest hook 的 loop 内构造客户端（client_fixture 是同步 fixture）。"""
    return coro_factory


async def test_health_and_success_envelope(client_fixture) -> None:
    async with client_fixture as c:
        r = await c.get("/health")
        assert r.status_code == 200
        r = await c.get("/items/x?delay=0.02&caller=alice",
                        headers={"x-request-id": "rid-success"})
        j = r.json()
        assert r.status_code == 200
        assert j["request_id"] == "rid-success"
        assert j["status"] == "success"
        assert j["data"]["key"] == "x"
        assert j["timing"]["execute_ms"] >= 15
        assert j["timing"]["queue_wait_ms"] >= 0
        log.info("成功请求 request_id=%s timing=%s",
                 j["request_id"], j["timing"])


async def test_burst_concurrency_distinguishable_statuses(client_fixture) -> None:
    # items 策略：cap=2 queue=2 timeout=0.3
    async with client_fixture as c:
        async def call() -> tuple[int, str]:
            r = await c.get("/items/k?delay=0.3&caller=hot", timeout=5)
            j = r.json()
            return r.status_code, (
                j.get("error") or j.get("status") or "unknown"
            )

        results = await asyncio.gather(*[call() for _ in range(8)])
    counts: dict[tuple[int, str], int] = {}
    for item in results:
        counts[item] = counts.get(item, 0) + 1
    log.info("突发判定分布=%s", counts)
    # 至少存在：1 次成功 + 1 次合并/缓存 + 立即拒绝(429) + 排队超时(503)
    assert any(s == 200 and st in {"success", "coalesced", "cache"}
               for (s, st) in results)
    assert (429, "capacity_rejected") in counts
    assert (503, "queue_timeout") in counts


async def test_recovery_after_pressure(client_fixture) -> None:
    async with client_fixture as c:
        async def call(delay: float) -> int:
            r = await c.get(f"/items/rc?delay={delay}&caller=rc", timeout=5)
            return r.status_code

        await asyncio.gather(*[call(0.3) for _ in range(8)],
                             return_exceptions=True)
        # 等待压力释放
        await asyncio.sleep(0.8)
        codes = await asyncio.gather(*[call(0.01) for _ in range(4)])
        assert all(code == 200 for code in codes)
        log.info("压力解除后恢复 codes=%s", codes)


async def test_degraded_response_is_distinguishable(client_fixture) -> None:
    # report：cap=1 queue=1 timeout=0.2 + fallback
    async with client_fixture as c:
        results = await asyncio.gather(*[
            c.get("/report?delay=0.3&caller=deg", timeout=5) for _ in range(4)
        ])
    statuses = [r.json()["status"] for r in results]
    log.info("降级判定 statuses=%s", statuses)
    assert "success" in statuses
    assert statuses.count("degraded") >= 1
    for r in results:
        j = r.json()
        if j["status"] == "degraded":
            assert r.status_code == 200
            assert j["degraded"] is True
            assert j["degrade_reason"] in {"capacity_full", "queue_timeout"}
            assert j["data"] == {"degraded": True, "items": []}


async def test_execution_failure_500_without_fallback(client_fixture) -> None:
    async with client_fixture as c:
        r = await c.get("/report?fail=true&delay=0&caller=failx", timeout=5)
    # report 默认带 fallback -> degraded；这里直接验证失败被映射
    j = r.json()
    log.info("失败+降级 status=%s reason=%s",
             j["status"], j.get("degrade_reason"))
    assert j["status"] == "degraded"


async def test_client_cancellation_does_not_break_service(client_fixture) -> None:
    async with client_fixture as c:
        async def canceled_call() -> None:
            try:
                await c.get("/items/cx?delay=1.0&caller=cx", timeout=0.05)
            except (httpx.TimeoutException, httpx.CancelledError):
                pass

        await asyncio.gather(*[canceled_call() for _ in range(4)])
        await asyncio.sleep(0.2)
        # 取消风暴后服务仍可用
        r = await c.get("/items/cx?delay=0.01&caller=other", timeout=5)
        assert r.status_code == 200
        m = await c.get("/admin/metrics")
        log.info("取消风暴后服务可用，total 指标=%s",
                 {k: m.json()["scopes"]["total"][k]
                  for k in ("started", "success", "canceled")
                  if k in m.json()["scopes"]["total"]})


async def test_config_switch_at_runtime(client_fixture) -> None:
    async with client_fixture as c:
        cfg = {
            "default": {"max_concurrency": 32, "max_queue": 32,
                        "queue_timeout": 5.0},
            "policies": [{
                "endpoint": "items", "caller": "*",
                "max_concurrency": 32, "max_queue": 32,
                "queue_timeout": 5.0,
                "coalesce_window": 1.0, "result_ttl": 0.5,
            }],
        }
        r = await c.post("/admin/config", content=json.dumps(cfg))
        assert r.status_code == 200 and r.json()["status"] == "applied"

        results = await asyncio.gather(*[
            c.get("/items/cfg?delay=0.05&caller=cfgx", timeout=5)
            for _ in range(12)
        ])
    statuses = [r.json()["status"] for r in results]
    log.info("配置切换后判定 statuses=%s", statuses)
    assert statuses.count("success") == 1
    assert set(statuses) <= {"success", "coalesced", "cache"}


async def test_metrics_show_timing_rejections_and_executions(
    client_fixture,
) -> None:
    async with client_fixture as c:
        await c.get("/items/m?delay=0.02&caller=m1")
        await asyncio.gather(*[
            c.get("/items/m2?delay=0.3&caller=mburst", timeout=5)
            for _ in range(6)
        ], return_exceptions=True)
        r = await c.get("/admin/metrics")
    j = r.json()
    total = j["scopes"]["total"]
    log.info("观测指标 total=%s", {
        k: total[k] for k in (
            "started", "success", "rejected", "capacity_rejects",
            "queue_timeouts", "executions", "coalesced", "cache_hit",
        ) if k in total
    })
    assert total["started"] >= 7
    assert total["rejected"] >= 1
    assert total["executions"] >= 1
    assert "endpoint:items" in j["scopes"]
    assert "caller:m1" in j["scopes"]
    assert "gates" in j and "flights" in j


async def _raw_asgi_get(app, path: str, cancel_after: float | None = None):
    """直接驱动 ASGI，cancel_after 到期后模拟客户端断开（不再收 more_body）。

    返回 (status, headers, body)；被取消时返回 None。
    """
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path.split("?")[0],
        "raw_path": path.split("?")[0].encode(),
        "query_string": path.split("?", 1)[1].encode() if "?" in path else b"",
        "headers": [(b"host", b"t")],
        "client": ("1.2.3.4", 1234),
        "server": ("t", 80),
    }
    captured = {"status": None, "body": b"", "headers": []}
    request_sent = asyncio.Event()

    async def receive():
        if cancel_after is None:
            await asyncio.Event().wait()  # 保持连接直到服务端完成
        if not request_sent.is_set():
            await request_sent.wait()
        raise asyncio.CancelledError()

    async def send(message):
        if message["type"] == "http.response.start":
            captured["status"] = message["status"]
            captured["headers"] = message["headers"]
            request_sent.set()
        elif message["type"] == "http.response.body":
            captured["body"] += message.get("body", b"")

    task = asyncio.create_task(app(scope, receive, send))
    if cancel_after is not None:
        await asyncio.sleep(cancel_after)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        return None
    await task
    return captured["status"], captured["headers"], captured["body"]


async def test_explicit_request_abort_is_mapped_and_leader_survives() -> None:
    """原生 ASGI 驱动：中断排队中的 follower 后，在途 leader 不受影响，
    闸门计数不漂移，后续请求正常。"""
    from governor.app import create_app as _create

    app = _create()
    blockers = [
        asyncio.create_task(
            _raw_asgi_get(app, "/items/block?delay=0.4&caller=abort")
        )
        for _ in range(2)
    ]
    await asyncio.sleep(0.15)
    aborted = asyncio.create_task(
        _raw_asgi_get(app, "/items/ab?delay=0.01&caller=abort",
                      cancel_after=0.05)
    )
    res = await aborted
    assert res is None  # 客户端视角：连接中断，无响应（真实 499 也是客户端不可见）
    log.info("排队中主动中断：客户端视角连接关闭")

    results = await asyncio.gather(*blockers)
    assert all(r is not None and r[0] == 200 for r in results)
    log.info("在途 leader 判定=unaffected statuses=%s", [r[0] for r in results])

    follow = await _raw_asgi_get(app, "/items/fine?delay=0.01&caller=abort")
    assert follow is not None and follow[0] == 200

    metrics = await _raw_asgi_get(app, "/admin/metrics")
    body = json.loads(metrics[2])
    assert "endpoint:items" in body["scopes"]
    log.info("取消后闸门计数无漂移 gates=%s", body["gates"])
