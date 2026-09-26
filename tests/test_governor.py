"""编排层：准入+合并+降级+指标的协同；配置切换不影响在途请求。"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging

from governor.config import ConfigStore, Policy, Snapshot, WILDCARD_CALLER
from governor.errors import ClientCanceled
from governor.governor import (
    STATUS_CACHE,
    STATUS_COALESCED,
    STATUS_DEGRADED,
    STATUS_REJECTED,
    STATUS_SUCCESS,
    Governor,
)

log = logging.getLogger("test.governor")

_id = itertools.count()


def rid() -> str:
    return f"req-{next(_id)}"


def store(policy: Policy | None = None, **policies: Policy) -> ConfigStore:
    pols = {}
    for key, p in policies.items():
        ep, _, caller = key.partition("|")
        pols[(ep, caller or WILDCARD_CALLER)] = p
    return ConfigStore(Snapshot(default=policy or Policy(), policies=pols))


def slow_work(delay: float, counter: dict[str, int] | None = None,
              result: object = "ok"):
    counter = counter if counter is not None else {"n": 0}

    async def f() -> object:
        counter["n"] += 1
        await asyncio.sleep(delay)
        return result

    return f


async def test_success_timing_breakdown() -> None:
    g = Governor(store(Policy(max_concurrency=2, max_queue=2,
                              queue_timeout=1)))
    out = await g.handle("ep", "c1", rid(), slow_work(0.03))
    assert out.status == STATUS_SUCCESS
    assert out.executed is True
    assert out.execute_time >= 0.02
    log.info("成功判定=%s queued=%.4fs execute=%.4fs total=%.4fs",
             out.status, out.queue_wait, out.execute_time, out.total_time)


async def test_burst_rejection_and_recovery() -> None:
    g = Governor(store(Policy(max_concurrency=2, max_queue=2,
                              queue_timeout=5)))
    release = asyncio.Event()

    async def hold() -> None:
        await g.handle("ep", "c", rid(),
                       lambda: _wait_event(release))

    holders = [asyncio.create_task(hold()) for _ in range(2)]
    queued = [asyncio.create_task(hold()) for _ in range(2)]
    await asyncio.sleep(0.05)
    # 第 5 个立即拒绝
    out = await g.handle("ep", "c", rid(), slow_work(0))
    assert out.status == STATUS_REJECTED
    assert out.reject_reason == "capacity_full"
    log.info("突发拒绝判定=%s reason=%s", out.status, out.reject_reason)
    release.set()
    await asyncio.gather(*holders, *queued)
    # 压力解除后恢复
    out = await g.handle("ep", "c", rid(), slow_work(0.01))
    assert out.status == STATUS_SUCCESS
    log.info("恢复判定=%s", out.status)


async def test_queue_timeout_then_degrade_or_reject() -> None:
    # 不降级：503 语义的 rejected/queue_timeout
    g = Governor(store(Policy(max_concurrency=1, max_queue=2,
                              queue_timeout=0.05)))
    release = asyncio.Event()
    holder = asyncio.create_task(
        g.handle("ep", "c", rid(), lambda: _wait_event(release))
    )
    await asyncio.sleep(0.02)
    out = await g.handle("ep", "c", rid(), slow_work(0))
    assert out.status == STATUS_REJECTED
    assert out.reject_reason == "queue_timeout"
    log.info("排队超时判定=%s queued=%.4fs", out.status, out.queue_wait)
    release.set()
    await holder

    # 降级策略：同样的容量压力返回兜底值且状态可区分
    g2 = Governor(store(Policy(
        max_concurrency=1, max_queue=1, queue_timeout=0.02,
        fallback_value={"stale": True})))
    rel2 = asyncio.Event()
    h = asyncio.create_task(
        g2.handle("ep", "c", rid(), lambda: _wait_event(rel2))
    )
    await asyncio.sleep(0.02)
    outs = await asyncio.gather(*[
        g2.handle("ep", "c", rid(), slow_work(0)) for _ in range(3)
    ])
    assert {o.status for o in outs} == {STATUS_DEGRADED}
    assert all(o.value == {"stale": True} for o in outs)
    reasons = {o.reject_reason for o in outs}
    assert reasons <= {"capacity_full", "queue_timeout"}
    log.info("容量降级判定=degraded reasons=%s", reasons)
    rel2.set()
    await h


async def test_coalesce_with_admission() -> None:
    g = Governor(store(Policy(
        max_concurrency=2, max_queue=8, queue_timeout=5,
        coalesce_window=1, result_ttl=0.3)))
    counter = {"n": 0}
    outs = await asyncio.gather(*[
        g.handle("items", "c", rid(),
                 slow_work(0.05, counter, "v"),
                 coalesce_key="items:k")
        for _ in range(5)
    ])
    assert counter["n"] == 1
    statuses = [o.status for o in outs]
    # 不变量：恰好一个实际执行，其余全部被合并/缓存吸收，无重复执行
    assert statuses.count(STATUS_SUCCESS) == 1
    assert statuses.count(STATUS_COALESCED) >= 1
    non_executed = sum(1 for o in outs if not o.executed)
    assert non_executed == 4
    assert all(o.value == "v" for o in outs)
    log.info("准入内合并判定 statuses=%s executions=1", statuses)

    # TTL 内：缓存命中
    out = await g.handle("items", "c", rid(),
                         slow_work(0.05, counter, "v"),
                         coalesce_key="items:k")
    assert out.status == STATUS_CACHE and counter["n"] == 1
    log.info("缓存判定=%s", out.status)
    await asyncio.sleep(0.32)
    out = await g.handle("items", "c", rid(),
                         slow_work(0.0, counter, "v"),
                         coalesce_key="items:k")
    assert out.status == STATUS_SUCCESS and counter["n"] == 2


async def test_execution_failure_degrade_or_failed() -> None:
    async def boom() -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("x")

    g = Governor(store(Policy()))
    out = await g.handle("ep", "c", rid(), boom)
    assert out.status == "failed" and out.executed is True
    log.info("执行失败判定=%s error=%s", out.status, type(out.cause).__name__)

    g2 = Governor(store(Policy(fallback_value="fallback")))
    out = await g2.handle("ep", "c", rid(), boom)
    assert out.status == STATUS_DEGRADED and out.value == "fallback"
    log.info("执行失败降级判定=%s", out.status)


async def test_client_cancel_is_mapped_and_does_not_break_limiter() -> None:
    g = Governor(store(Policy(max_concurrency=1, max_queue=3,
                              queue_timeout=5)))
    release = asyncio.Event()
    holder = asyncio.create_task(
        g.handle("ep", "c", rid(), lambda: _wait_event(release))
    )
    await asyncio.sleep(0.02)
    t = asyncio.create_task(
        g.handle("ep", "c", rid(), slow_work(0))
    )
    await asyncio.sleep(0.02)
    t.cancel()
    with contextlib.suppress(ClientCanceled, asyncio.CancelledError):
        await t
    release.set()
    await holder
    # 槽位与队列计数无漂移：后续请求正常
    out = await g.handle("ep", "c", rid(), slow_work(0.01))
    assert out.status == STATUS_SUCCESS
    snap = g.metrics.snapshot()
    assert snap["total"]["canceled"] >= 1
    log.info("取消判定=canceled 后续=%s", out.status)


async def test_config_switch_does_not_change_inflight_semantics() -> None:
    cfg = store(Policy(max_concurrency=1, max_queue=1, queue_timeout=5))
    g = Governor(cfg)
    release = asyncio.Event()

    async def work() -> str:
        await release.wait()
        return "done-under-old-policy"

    # 在旧配置下进入执行
    t = asyncio.create_task(g.handle("ep", "c", rid(), work))
    await asyncio.sleep(0.03)
    # 运行时把配置改成“零容忍”（立即拒绝，无降级），在途请求不受影响
    cfg.replace(Snapshot(default=Policy(
        max_concurrency=1, max_queue=0, queue_timeout=0.0)))
    g.reload()
    # 同一 gate 下：并发已被在途请求占用，队列容量为 0 -> 立即拒绝
    same_caller = await g.handle("ep", "c", rid(), slow_work(0))
    assert same_caller.status == STATUS_REJECTED
    assert same_caller.reject_reason == "capacity_full"
    log.info("配置切换中新请求判定=%s", same_caller.status)
    # 其他调用方是独立 gate，不受该接口在途请求影响
    other_caller = await g.handle("ep", "c2", rid(), slow_work(0.01))
    assert other_caller.status == STATUS_SUCCESS
    release.set()
    out = await t
    assert out.status == STATUS_SUCCESS
    assert out.value == "done-under-old-policy"
    log.info("在途请求判定=unaffected status=%s", out.status)


async def _wait_event(ev: asyncio.Event) -> str:
    await ev.wait()
    return "released"
