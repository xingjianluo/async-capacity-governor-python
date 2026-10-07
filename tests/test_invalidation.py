"""主动作废 / 强制刷新：
- 正在合并执行时作废：所有等待者收到可区分的 invalidated，旧值被丢弃；
- 已缓存未过期时作废：后续请求强制重新执行，不被旧值污染；
- 作废不存在 / 重复作废：安全无副作用；
- 作废与并发准入同时发生：拒绝/降级语义不走样，压力过后恢复正常；
- 作废不拖累其它 key 的在途请求，不引起重复执行与计数漂移。
"""

from __future__ import annotations

import asyncio
import itertools
import logging

import pytest

from governor.coalescer import Coalescer
from governor.config import ConfigStore, Policy, Snapshot, WILDCARD_CALLER
from governor.errors import ResultInvalidated
from governor.governor import (
    STATUS_CACHE,
    STATUS_INVALIDATED,
    STATUS_REJECTED,
    STATUS_SUCCESS,
    Governor,
)

log = logging.getLogger("test.invalidation")

_id = itertools.count()


def rid() -> str:
    return f"req-inv-{next(_id)}"


def store(policy: Policy) -> ConfigStore:
    return ConfigStore(Snapshot(default=policy, policies={}))


class Counter:
    def __init__(self) -> None:
        self.n = 0
        self.started = asyncio.Event()

    def factory(self, result: object = "v", delay: float = 0.1,
                gate: asyncio.Event | None = None):
        started = self.started

        async def f() -> object:
            self.n += 1
            started.set()
            if gate is not None:
                await gate.wait()
            await asyncio.sleep(delay)
            return result

        return f


# ---------- coalescer 层 ----------

async def test_invalidate_inflight_coalesced_execution() -> None:
    c = Coalescer()
    counter = Counter()
    release = asyncio.Event()
    tasks = [
        asyncio.create_task(
            c.run("k", counter.factory(result="old", delay=0.0, gate=release),
                  ttl=10.0)
        )
        for _ in range(3)
    ]
    await counter.started.wait()
    assert c.invalidate("k") is True
    log.info("在途作废判定=applied flight_remaining=%d", c.flight_count())
    assert c.flight_count() == 0, "作废后 flight 应立即摘除"
    release.set()
    for t in tasks:
        with pytest.raises(ResultInvalidated):
            await t
    log.info("在途等待者判定=all-invalidated waiters=%d", len(tasks))
    # 作废本身不引起后端重复执行：只有 leader 执行过一次
    assert counter.n == 1
    # 后续请求重新执行拿到最新值，不被旧值污染
    fresh = await c.run("k", counter.factory(result="new", delay=0.0), ttl=10.0)
    assert fresh == ("new", True, False)
    assert counter.n == 2
    log.info("作善后重算判定=fresh-executions=%d", counter.n)


async def test_invalidate_cached_result_forces_refresh() -> None:
    c = Coalescer()
    counter = Counter()
    first = await c.run("k", counter.factory(result="v1", delay=0.01), ttl=10.0)
    assert first == ("v1", True, False)
    # 未过期时本应命中缓存；作废后必须重算
    assert c.invalidate("k") is True
    second = await c.run("k", counter.factory(result="v2", delay=0.01), ttl=10.0)
    assert second == ("v2", True, False)
    assert counter.n == 2
    log.info("缓存作废判定=re-executed executions=%d", counter.n)


async def test_invalidate_nonexistent_and_repeated_is_safe() -> None:
    c = Coalescer()
    assert c.invalidate("missing") is False
    assert c.flight_count() == 0
    counter = Counter()
    await c.run("k", counter.factory(delay=0.01), ttl=10.0)
    assert c.invalidate("k") is True
    assert c.invalidate("k") is False, "重复作废必须无副作用"
    assert c.flight_count() == 0
    log.info("空作废/重复作废判定=safe-noop")


async def test_invalidate_does_not_disturb_other_inflight_keys() -> None:
    c = Coalescer()
    ca, cb = Counter(), Counter()
    ta = asyncio.create_task(c.run("a", ca.factory(result="ra", delay=0.08), 1.0))
    tb = asyncio.create_task(c.run("b", cb.factory(result="rb", delay=0.08), 1.0))
    await ca.started.wait()
    await cb.started.wait()
    assert c.invalidate("a") is True
    with pytest.raises(ResultInvalidated):
        await ta
    assert (await tb)[0] == "rb", "其它 key 的在途请求不得被拖累"
    assert cb.n == 1
    log.info("隔离判定=other-key-unaffected")


# ---------- governor 编排层 ----------

async def test_governor_invalidate_during_coalesced_execution() -> None:
    g = Governor(store(Policy(max_concurrency=4, max_queue=8, queue_timeout=5,
                              coalesce_window=1, result_ttl=10)))
    counter = {"n": 0}
    started = asyncio.Event()
    release = asyncio.Event()

    async def work() -> str:
        counter["n"] += 1
        started.set()
        await release.wait()
        return "old"

    tasks = [
        asyncio.create_task(
            g.handle("items", "c", rid(), work, coalesce_key="items:k"))
        for _ in range(3)
    ]
    await started.wait()
    assert g.invalidate("items", "c", "items:k") is True
    release.set()
    outs = await asyncio.gather(*tasks)
    statuses = [o.status for o in outs]
    assert statuses == [STATUS_INVALIDATED] * 3, (
        "所有等待者必须收到可区分的 invalidated，而非成功/拒绝/取消")
    assert sum(1 for o in outs if o.executed) == 1, (
        "只有 leader 计入实际执行，follower 不得重复执行")
    log.info("编排层在途作废判定 statuses=%s executions=%d",
             statuses, counter["n"])
    assert counter["n"] == 1, "作废不得引起后端重复执行"
    # 后续请求重新执行拿到最新值
    out = await g.handle("items", "c", rid(),
                         lambda: _fresh_work(counter, "new"), coalesce_key="items:k")
    assert out.status == STATUS_SUCCESS and out.value == "new"
    assert counter["n"] == 2
    snap = g.metrics.snapshot()
    assert snap["total"]["invalidated"] == 3
    assert snap["total"]["invalidations"] == 1
    assert snap["total"]["executions"] == 2, "执行计数不得漂移"
    log.info("指标判定 invalidated=%d invalidations=%d executions=%d",
             snap["total"]["invalidated"], snap["total"]["invalidations"],
             snap["total"]["executions"])


async def test_governor_invalidate_cached_then_refresh() -> None:
    g = Governor(store(Policy(max_concurrency=2, max_queue=4, queue_timeout=1,
                              coalesce_window=1, result_ttl=10)))
    counter = {"n": 0}
    out1 = await g.handle("items", "c", rid(),
                          lambda: _fresh_work(counter, "v1"), coalesce_key="items:k")
    assert out1.status == STATUS_SUCCESS
    out2 = await g.handle("items", "c", rid(),
                          lambda: _fresh_work(counter, "v1"), coalesce_key="items:k")
    assert out2.status == STATUS_CACHE and counter["n"] == 1
    log.info("缓存命中判定=%s executions=%d", out2.status, counter["n"])
    assert g.invalidate("items", "c", "items:k") is True
    out3 = await g.handle("items", "c", rid(),
                          lambda: _fresh_work(counter, "v2"), coalesce_key="items:k")
    assert out3.status == STATUS_SUCCESS and out3.value == "v2"
    assert counter["n"] == 2, "作废后必须重新执行，不得复用旧缓存"
    log.info("强制刷新判定=%s value=%s executions=%d",
             out3.status, out3.value, counter["n"])


async def test_governor_invalidate_nonexistent_is_safe() -> None:
    g = Governor(store(Policy(coalesce_window=1, result_ttl=1)))
    assert g.invalidate("items", "c", "items:missing") is False
    assert g.invalidate("items", "c", "items:missing") is False
    counter = {"n": 0}
    out = await g.handle("items", "c", rid(),
                         lambda: _fresh_work(counter, "ok"), coalesce_key="items:k")
    assert out.status == STATUS_SUCCESS
    snap = g.metrics.snapshot()
    assert snap["total"]["invalidations"] == 0
    log.info("空作废判定=noop 后续请求=%s", out.status)


async def test_invalidate_under_capacity_pressure() -> None:
    """作废与容量打满同时发生：拒绝语义不走样，压力过后恢复正常。"""
    g = Governor(store(Policy(max_concurrency=2, max_queue=1, queue_timeout=0.5,
                              coalesce_window=1, result_ttl=10)))
    counter = {"n": 0}
    started = asyncio.Event()
    release = asyncio.Event()

    async def work() -> str:
        counter["n"] += 1
        started.set()
        await release.wait()
        return "old"

    # 2 个并发占满执行槽：leader 执行 + follower 等待同一 flight
    t1 = asyncio.create_task(
        g.handle("items", "hot", rid(), work, coalesce_key="items:k"))
    t2 = asyncio.create_task(
        g.handle("items", "hot", rid(), work, coalesce_key="items:k"))
    await started.wait()
    await asyncio.sleep(0.02)
    # 第 3 个进入有界排队，第 4 个容量打满立即拒绝
    t3 = asyncio.create_task(
        g.handle("items", "hot", rid(), work, coalesce_key="items:k"))
    await asyncio.sleep(0.02)
    out_rej = await g.handle("items", "hot", rid(), work,
                             coalesce_key="items:k")
    assert out_rej.status == STATUS_REJECTED
    assert out_rej.reject_reason == "capacity_full"
    log.info("容量打满判定=%s reason=%s", out_rej.status, out_rej.reject_reason)
    # 容量打满的同时发起作废
    assert g.invalidate("items", "hot", "items:k") is True
    release.set()
    o1, o2 = await asyncio.gather(t1, t2)
    assert o1.status == STATUS_INVALIDATED
    assert o2.status == STATUS_INVALIDATED
    # 排队中的 t3 在作废后才进入合并层：视为新请求，重新执行拿最新值
    o3 = await t3
    assert o3.status == STATUS_SUCCESS, (
        "作废后才到达合并层的请求应成为新 leader 重新执行")
    log.info("排队者恢复判定=%s executions=%d", o3.status, counter["n"])
    # 压力过后：槽位无泄漏，新请求正常
    out = await g.handle("items", "hot", rid(),
                         lambda: _fresh_work(counter, "new"), coalesce_key="items:k2")
    assert out.status == STATUS_SUCCESS and out.value == "new"
    states = g.limiter.states()
    assert all(a == 0 and w == 0 for a, w in states.values()), (
        "压力过后槽位与队列计数必须归零")
    log.info("恢复判定=%s gate_states=%s", out.status, states)


async def test_consecutive_invalidations_then_recover() -> None:
    """短时间内连续作废：语义不走样，压力过后恢复正常。"""
    g = Governor(store(Policy(max_concurrency=2, max_queue=4, queue_timeout=1,
                              coalesce_window=1, result_ttl=10)))
    counter = {"n": 0}
    for round_no in range(3):
        out = await g.handle("items", "c", rid(),
                             lambda: _fresh_work(counter, f"v{round_no}"),
                             coalesce_key="items:k")
        assert out.status in (STATUS_SUCCESS, STATUS_CACHE)
        assert g.invalidate("items", "c", "items:k") is True
    # 连续作废后仍能正常重算
    out = await g.handle("items", "c", rid(),
                         lambda: _fresh_work(counter, "final"), coalesce_key="items:k")
    assert out.status == STATUS_SUCCESS and out.value == "final"
    snap = g.metrics.snapshot()
    assert snap["total"]["invalidations"] == 3
    log.info("连续作废判定 invalidations=%d 恢复=%s",
             snap["total"]["invalidations"], out.status)


async def _fresh_work(counter: dict[str, int], result: object) -> object:
    counter["n"] += 1
    await asyncio.sleep(0.01)
    return result


# ---------- HTTP 层 ----------

async def test_http_invalidate_endpoint_and_409_mapping() -> None:
    import httpx

    from governor.app import create_app

    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://t") as client:
        # 慢请求在途（合并窗口内），随后作废
        async def slow_get() -> tuple[int, dict]:
            r = await client.get("/items/hot-inv?delay=0.3&caller=ops",
                                 headers={"x-request-id": "rid-inv-wait"},
                                 timeout=5)
            return r.status_code, r.json()

        task = asyncio.create_task(slow_get())
        await asyncio.sleep(0.05)
        r = await client.post("/admin/invalidate", json={
            "endpoint": "items", "caller": "ops",
            "coalesce_key": "items:hot-inv",
        })
        assert r.status_code == 200 and r.json()["applied"] is True
        code, body = await task
        assert code == 409
        assert body["status"] == "invalidated"
        assert body["error"] == "result_invalidated"
        assert body["request_id"] == "rid-inv-wait"
        log.info("HTTP 在途作废判定 code=%d body=%s", code, body)
        # 重复作废：幂等无副作用
        r = await client.post("/admin/invalidate", json={
            "endpoint": "items", "caller": "ops",
            "coalesce_key": "items:hot-inv",
        })
        assert r.json()["applied"] is False
        # 作废不存在的结果：安全
        r = await client.post("/admin/invalidate", json={
            "endpoint": "items", "caller": "ops",
            "coalesce_key": "items:never-existed",
        })
        assert r.json()["applied"] is False
        # 缺参数：400
        r = await client.post("/admin/invalidate", json={"endpoint": "items"})
        assert r.status_code == 400
        # 作废后新请求重新执行拿到最新结果
        r = await client.get("/items/hot-inv?delay=0.01&caller=ops",
                             headers={"x-request-id": "rid-inv-fresh"})
        assert r.status_code == 200 and r.json()["status"] == "success"
        log.info("HTTP 刷新后判定=%s", r.json()["status"])
