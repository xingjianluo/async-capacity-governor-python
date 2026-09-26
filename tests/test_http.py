"""End-to-end HTTP behavior: distinguishable outcomes, coalescing, config swap."""

import asyncio
import json
import logging

import httpx
import pytest

from capacity_governor import CapacityGovernor, GovernorConfig, RouteLimit
from main import create_app

logger = logging.getLogger("test.http")


@pytest.fixture()
def env():
    gov = CapacityGovernor(GovernorConfig(
        default_limit=RouteLimit(max_concurrent=2, max_queued=1,
                                 queue_timeout_s=0.1,
                                 coalesce_enabled=True, coalesce_ttl_s=0.3)))
    app = create_app(gov)
    transport = httpx.ASGITransport(app=app)
    return gov, transport


async def test_success_rejected_degraded_are_distinguishable(env):
    gov, transport = env
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        # success
        r = await c.get("/items/1", params={"delay": 0.01},
                        headers={"x-caller": "alice", "x-request-id": "ok1"})
        body = r.json()
        assert r.status_code == 200 and body["status"] == "success"
        assert body["value"]["payload"] == "item-1"
        assert body["queue_wait_s"] >= 0
        logger.info("success body=%s", body)

        # burst: 3 slow at once -> 2 run, 1 rejected (no fallback)
        rs = await asyncio.gather(*[
            c.get("/items/9", params={"delay": 0.3, "degrade": "false"},
                  headers={"x-caller": "spammer", "x-request-id": f"r{i}"})
            for i in range(4)])
        statuses = sorted(r.status_code for r in rs)
        # 2 run, 1 queues then times out (503), 1 rejected immediately (429)
        assert statuses == [200, 200, 429, 503], statuses
        rejected = [r for r in rs if r.status_code in (429, 503)]
        assert all(r.json()["status"] == "rejected" for r in rejected)
        reasons = sorted(r.json()["reason"] for r in rejected)
        assert reasons == ["queue_full", "queue_timeout"], reasons
        logger.info("burst statuses=%s reasons=%s", statuses, reasons)

        # degraded returns 200 but an explicit status field
        gov.update_config(GovernorConfig(
            default_limit=RouteLimit(max_concurrent=1, max_queued=0,
                                     queue_timeout_s=0.05)))
        holder = asyncio.create_task(c.get(
            "/items/8", params={"delay": 0.3},
            headers={"x-caller": "x", "x-request-id": "holder"}))
        await asyncio.sleep(0.05)
        r = await c.get("/items/8",
                        headers={"x-caller": "x", "x-request-id": "deg"})
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "degraded"
        assert body["reason"] == "queue_full"
        assert body["value"]["payload"] == "stale-fallback"
        logger.info("degraded body=%s", body)
        await holder


async def test_queue_timeout_returns_503(env):
    gov, transport = env
    gov.update_config(GovernorConfig(
        default_limit=RouteLimit(max_concurrent=1, max_queued=1,
                                 queue_timeout_s=0.05, coalesce_enabled=False)))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        holders = [asyncio.create_task(c.get(
            "/items/7", params={"delay": 0.4, "degrade": "false"},
            headers={"x-caller": "z", "x-request-id": f"h{i}"}))
            for i in range(1)]
        await asyncio.sleep(0.05)
        r = await c.get("/items/7", params={"delay": 0.4, "degrade": "false"},
                        headers={"x-caller": "z", "x-request-id": "victim"})
        assert r.status_code == 503
        assert r.json()["reason"] == "queue_timeout"
        logger.info("timeout body=%s", r.json())
        await asyncio.gather(*holders)


async def test_identical_requests_coalesce_over_http(env):
    _, transport = env
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        rs = await asyncio.gather(*[
            c.get("/items/3", params={"delay": 0.1},
                  headers={"x-caller": f"caller-{i}", "x-request-id": f"m{i}"})
            for i in range(4)])
        shared = [r.json()["shared"] for r in rs]
        assert shared.count(False) == 1 and shared.count(True) == 3, shared
        assert all(r.json()["status"] == "success" for r in rs)
        logger.info("coalesce shared=%s", shared)


async def test_invalidate_conflict_409(env):
    _, transport = env
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        slow = asyncio.create_task(c.get(
            "/items/5", params={"delay": 5.0},
            headers={"x-caller": "z", "x-request-id": "slow"}))
        waiter = asyncio.create_task(c.get(
            "/items/5", params={"delay": 5.0},
            headers={"x-caller": "y", "x-request-id": "waiter"}))
        await asyncio.sleep(0.05)
        r = await c.post("/admin/invalidate/item:5")
        assert r.json() == {"invalidated": "item:5"}
        for t in (slow, waiter):
            with pytest.raises(httpx.HTTPStatusError) if False else __import__("contextlib").nullcontext():
                pass
        results = await asyncio.gather(slow, waiter, return_exceptions=True)
        for res in results:
            if isinstance(res, httpx.Response):
                assert res.status_code == 409
                assert res.json()["reason"] == "concurrency_conflict"
            else:
                raise res
        # later request unaffected
        r = await c.get("/items/5", params={"delay": 0.01})
        assert r.status_code == 200 and r.json()["status"] == "success"
        logger.info("invalidate conflict verified")


async def test_client_cancel_over_http_does_not_drift(env):
    gov, transport = env
    gov.update_config(GovernorConfig(
        default_limit=RouteLimit(max_concurrent=1, max_queued=2,
                                 queue_timeout_s=5.0, coalesce_enabled=False)))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        holder = asyncio.create_task(c.get(
            "/items/6", params={"delay": 0.4, "degrade": "false"},
            headers={"x-caller": "z", "x-request-id": "holder"}))
        await asyncio.sleep(0.05)
        victim = asyncio.create_task(c.get(
            "/items/6", params={"delay": 0.4, "degrade": "false"},
            headers={"x-caller": "z", "x-request-id": "victim"}))
        await asyncio.sleep(0.05)
        victim.cancel()
        try:
            await victim  # ASGI transport may shield the client-side cancel
        except asyncio.CancelledError:
            pass
        await holder
        # pressure gone; service recovers, no count/queue drift
        r = await c.get("/items/6", params={"delay": 0.01})
        assert r.status_code == 200
        assert gov.lane_stats()["/items/{item_id}:z"] == {"running": 0,
                                                          "waiting": 0}
        assert gov.metrics.snapshot()["counters"]["cancelled"] >= 1
        logger.info("client cancel handled without drift")


async def test_config_switch_and_metrics(env):
    gov, transport = env
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/config")
        assert r.json()["version"] == 0

        r = await c.put("/config", json={
            "default_limit": {"max_concurrent": 1, "max_queued": 0,
                              "queue_timeout_s": 0.05,
                              "coalesce_enabled": False}})
        assert r.json()["version"] == 1

        # new semantics apply: 2 concurrent -> 1 succeeds, 1 rejected
        rs = await asyncio.gather(*[
            c.get("/items/4", params={"delay": 0.2, "degrade": "false"},
                  headers={"x-caller": "same-caller",
                           "x-request-id": f"sw{i}"}) for i in range(2)])
        codes = sorted(r.status_code for r in rs)
        assert codes == [200, 429], codes
        logger.info("config-switch codes=%s", codes)

        m = (await c.get("/metrics")).json()
        logger.info("metrics=%s", json.dumps(m))
        assert m["config_version"] == 1
        assert m["counters"]["rejected.queue_full"] >= 1
        assert "queue_wait" in m["latencies"]
        assert "execute" in m["latencies"]
        assert "/items/{item_id}:same-caller" in m["lanes"]
