"""主动作废 / 强制刷新：

- 正在合并执行时作废：全部等待者立即收到可区分的 ResultInvalidated，
  后端不重复执行、槽位不泄漏，后续请求重新执行拿到新值；
- 已缓存未过期时作废：缓存被丢弃，下一请求重新执行；
- 作废不存在 / 重复作废：安全无副作用；
- 作废与并发准入同时发生：拒绝/降级语义不走样，压力过后恢复正常。
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging

import pytest

from governor.app import create_app
from governor.coalescer import Coalescer
from governor.config import ConfigStore, Policy, Snapshot
from governor.errors import ResultInvalidated
from governor.governor import (
    STATUS_INVALIDATED,
    STATUS_REJECTED,
    STATUS_SUCCESS,
    Governor,
)

log = logging.getLogger("test.invalidate")

_id = itertools.count()


def rid() -> str:
    return f"inv-{next(_id)}"


def store(policy: Policy) -> ConfigStore:
    return ConfigStore(Snapshot(default=policy))


def slow_work(delay: float, counter: dict[str, int], result: object = "ok"):
    async def f() -> object:
        counter["n"] += 1
        await asyncio.sleep(delay)
        return result

    return f


class Counter:
    def __init__(self) -> None:
        self.n = 0
        self.started = asyncio.Event()

    def factory(self, result: object = "v", delay: float = 0.1):
        started = self.started

        async def f() -> object:
            self.n += 1
            started.set()
            await asyncio.sleep(delay)
            return result

        return f


async def test_invalidate_inflight_wakes_waiters_without_reexecution() -> None:
    """正在合并执行时作废：等待者立即收到 ResultInvalidated，后端不重复执行。"""
    c = Coalescer()
    counter = Counter()
    t1 = asyncio.create_task(c.run("k", counter.factory(result="v1", delay=0.3), 10))
    await counter.started.wait()
    t2 = asyncio.create_task(c.run("k", counter.factory(delay=0.3), 10))
    t3 = asyncio.create_task(c.run("k", counter.factory(delay=0.3), 10))
    await asyncio.sleep(0.02)

    assert c.invalidate("k") is True
    for t in (t1, t2, t3):
        with pytest.raises(ResultInvalidated):
            # 0.3s 的执行远未结束：能被唤醒即证明是作废生效而非正常完成
            await asyncio.wait_for(t, 0.5)
    assert counter.n == 1, "作废不得引起后端重复执行"
    assert c.flight_count() == 0, "作废后不得残留可被命中的 flight"
    log.info("在途作废判定=all-waiters-invalidated executions=%d", counter.n)

    # 作废后的新请求必须重新执行，绝拿不到作废前的旧值
    r = await c.run("k", counter.factory(result="v2", delay=0.01), 10)
    assert r == ("v2", True, False)
    assert counter.n == 2
    log.info("作废后刷新判定=re-executed value=%s", r[0])


async def test_invalidate_cached_result_forces_refresh() -> None:
    """已缓存未过期时作废：缓存被丢弃，下一请求重新执行。"""
    c = Coalescer()
    counter = Counter()
    r1 = await c.run("k", counter.factory(result="old", delay=0.01), ttl=10)
    assert r1 == ("old", True, False)
    hit = await c.run("k", counter.factory(result="old", delay=0.01), ttl=10)
    assert hit == ("old", False, True), "TTL 内应命中缓存"
    assert counter.n == 1

    assert c.invalidate("k") is True
    r3 = await c.run("k", counter.factory(result="new", delay=0.01), ttl=10)
    assert r3 == ("new", True, False), "作废后必须重新执行并拿到新值"
    assert counter.n == 2
    log.info("缓存作废判定=forced-refresh executions=%d", counter.n)


async def test_invalidate_nonexistent_and_repeat_is_safe() -> None:
    """作废不存在的结果、重复作废：安全无副作用，正常流程不受影响。"""
    c = Coalescer()
    assert c.invalidate("nope") is False
    assert c.invalidate("nope") is False
    assert c.flight_count() == 0

    counter = Counter()
    await c.run("k", counter.factory(result="v", delay=0.01), ttl=10)
    assert c.invalidate("k") is True
    assert c.invalidate("k") is False, "同一对象重复作废不得有副作用"
    assert c.flight_count() == 0

    r = await c.run("k", counter.factory(result="v2", delay=0.01), ttl=10)
    assert r == ("v2", True, False)
    log.info("空作废/重复作废判定=no-op executions=%d", counter.n)


async def test_repeated_invalidation_across_flights() -> None:
    """短时间内连续作废：每一轮 flight 的等待者都被正确作废，互不污染。"""
    c = Coalescer()
    counter = Counter()
    t1 = asyncio.create_task(c.run("k", counter.factory(result="v1", delay=0.3), 10))
    await counter.started.wait()
    assert c.invalidate("k") is True
    with pytest.raises(ResultInvalidated):
        await t1

    t2 = asyncio.create_task(c.run("k", counter.factory(result="v2", delay=0.3), 10))
    await asyncio.sleep(0.02)
    assert c.invalidate("k") is True, "新一轮 flight 应能被再次作废"
    with pytest.raises(ResultInvalidated):
        await t2

    r = await c.run("k", counter.factory(result="v3", delay=0.01), 10)
    assert r == ("v3", True, False)
    assert counter.n == 3
    log.info("连续作废判定=isolated-per-flight executions=%d", counter.n)


async def test_governor_invalidate_during_coalesced_execution() -> None:
    """编排层：合并执行中作废 -> 全部等待者得到 invalidated 可区分状态，
    槽位正常释放，后续请求重新执行，指标不漂移。"""
    g = Governor(store(Policy(
        max_concurrency=4, max_queue=8, queue_timeout=5,
        coalesce_window=1, result_ttl=10)))
    counter = {"n": 0}
    release = asyncio.Event()

    async def slow() -> str:
        counter["n"] += 1
        await release.wait()
        return f"v{counter['n']}"

    tasks = [
        asyncio.create_task(
            g.handle("items", "c", rid(), slow, coalesce_key="items:k")
        )
        for _ in range(3)
    ]
    await asyncio.sleep(0.05)
    assert counter["n"] == 1

    assert g.invalidate("items:k", endpoint="items", caller="c") is True
    # release 尚未置位：能返回即证明等待者被作废立即唤醒
    outs = await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
    assert all(o.status == STATUS_INVALIDATED for o in outs)
    assert all(o.value is None for o in outs), "作废后绝不下发旧值"
    log.info("编排层在途作废判定 statuses=%s", [o.status for o in outs])

    # 槽位无泄漏：作废返回后准入计数立即归零（后台执行不占用槽位）
    assert g.limiter.states()[("items", "c")] == (0, 0)

    release.set()
    await asyncio.sleep(0.05)  # 让旧的后台执行完成（结果应被丢弃）
    out = await g.handle("items", "c", rid(), slow, coalesce_key="items:k")
    assert out.status == STATUS_SUCCESS
    assert out.value == "v2", "作废后必须拿到重新计算的新值"
    assert counter["n"] == 2

    total = g.metrics.snapshot()["total"]
    assert total["invalidated"] == 3
    assert total["invalidations"] == 1
    assert total["executions"] == 2, "旧 leader + 新 leader 各执行一次，无漂移"
    log.info("指标判定 invalidated=%d invalidations=%d executions=%d",
             total["invalidated"], total["invalidations"], total["executions"])


async def test_invalidate_under_capacity_pressure() -> None:
    """作废与并发准入同时发生：拒绝语义不走样，作废释放槽位后新请求
    重新执行并恢复正常。"""
    g = Governor(store(Policy(
        max_concurrency=1, max_queue=1, queue_timeout=0.5,
        coalesce_window=1, result_ttl=10)))
    counter = {"n": 0}
    release1 = asyncio.Event()

    async def slow1() -> str:
        counter["n"] += 1
        await release1.wait()
        return "stale"

    t1 = asyncio.create_task(
        g.handle("items", "c", rid(), slow1, coalesce_key="items:k")
    )
    await asyncio.sleep(0.05)
    t2 = asyncio.create_task(
        g.handle("items", "c", rid(),
                 slow_work(0.01, counter, "fresh"), coalesce_key="items:k")
    )
    await asyncio.sleep(0.05)

    # 容量打满（1 在执行 + 1 在排队）：第三个请求立即拒绝，语义不走样
    out3 = await g.handle("items", "c", rid(),
                          slow_work(0, counter), coalesce_key="items:k")
    assert out3.status == STATUS_REJECTED
    assert out3.reject_reason == "capacity_full"
    log.info("压力下拒绝判定=%s reason=%s", out3.status, out3.reject_reason)

    # 作废：t1 立即收到 invalidated 并释放槽位；t2 准入后成为新 leader 重新执行
    assert g.invalidate("items:k", endpoint="items", caller="c") is True
    out1 = await asyncio.wait_for(t1, timeout=1)
    assert out1.status == STATUS_INVALIDATED
    out2 = await asyncio.wait_for(t2, timeout=1)
    assert out2.status == STATUS_SUCCESS
    assert out2.value == "fresh", "作废后准入的请求必须重新执行拿新值"
    release1.set()
    await asyncio.sleep(0.05)

    # 压力过后：连续作废安全，请求恢复正常
    assert g.invalidate("items:k") is True   # t2 的结果仍在 TTL 内
    assert g.invalidate("items:k") is False
    out = await g.handle("items", "c", rid(),
                         slow_work(0.01, counter, "newest"),
                         coalesce_key="items:k")
    assert out.status == STATUS_SUCCESS and out.value == "newest"

    total = g.metrics.snapshot()["total"]
    assert total["capacity_rejects"] == 1
    assert total["invalidated"] == 1
    assert total["executions"] == counter["n"] == 3
    assert g.limiter.states()[("items", "c")] == (0, 0)
    log.info("压力+作废恢复判定 executions=%d rejects=%d invalidated=%d",
             total["executions"], total["capacity_rejects"], total["invalidated"])


async def test_http_invalidate_cached_then_refresh() -> None:
    """HTTP：作废已缓存结果 -> 后续请求重新执行；重复/空 key 作废安全。"""
    import httpx

    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r1 = await c.get("/items/inv?delay=0.02&caller=inv",
                         headers={"x-request-id": "http-inv-1"})
        assert r1.json()["status"] == "success"
        r2 = await c.get("/items/inv?delay=0.02&caller=inv",
                         headers={"x-request-id": "http-inv-2"})
        assert r2.json()["status"] == "cache"

        ri = await c.post("/admin/invalidate", content=json.dumps(
            {"key": "items:inv", "endpoint": "items", "caller": "inv"}))
        assert ri.status_code == 200 and ri.json()["invalidated"] is True
        # 重复作废 / 不存在的 key：安全无副作用
        ri2 = await c.post("/admin/invalidate", content=json.dumps(
            {"key": "items:inv"}))
        assert ri2.json()["invalidated"] is False
        ri3 = await c.post("/admin/invalidate", content=json.dumps(
            {"key": "items:nope"}))
        assert ri3.json()["invalidated"] is False
        bad = await c.post("/admin/invalidate", content=json.dumps({}))
        assert bad.status_code == 400

        r3 = await c.get("/items/inv?delay=0.02&caller=inv",
                         headers={"x-request-id": "http-inv-3"})
        j3 = r3.json()
        assert r3.status_code == 200
        assert j3["status"] == "success" and j3["executed"] is True
        log.info("HTTP 缓存作废判定 request_id=%s status=%s timing=%s",
                 j3["request_id"], j3["status"], j3["timing"])


async def test_http_invalidate_inflight_returns_409() -> None:
    """HTTP：在途合并执行被作废 -> 等待者收到 409 result_invalidated，
    与 429/503/499 明显区分；之后请求恢复正常。"""
    import httpx

    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        task = asyncio.create_task(c.get(
            "/items/slow?delay=0.4&caller=inv2",
            headers={"x-request-id": "http-inv-inflight"}))
        await asyncio.sleep(0.1)
        ri = await c.post("/admin/invalidate", content=json.dumps(
            {"key": "items:slow", "endpoint": "items"}))
        assert ri.json()["invalidated"] is True

        r = await task
        j = r.json()
        assert r.status_code == 409
        assert j["status"] == "invalidated"
        assert j["error"] == "result_invalidated"
        log.info("HTTP 在途作废判定 request_id=%s status=%d error=%s timing=%s",
                 j["request_id"], r.status_code, j["error"], j["timing"])

        # 作废后新请求重新执行，恢复正常
        r2 = await c.get("/items/slow?delay=0.01&caller=inv2",
                         headers={"x-request-id": "http-inv-after"})
        assert r2.status_code == 200 and r2.json()["status"] == "success"

        m = await c.get("/admin/metrics")
        total = m.json()["scopes"]["total"]
        assert total["invalidated"] >= 1
        assert total["invalidations"] >= 1
        log.info("HTTP 作废指标 invalidated=%d invalidations=%d",
                 total["invalidated"], total["invalidations"])
