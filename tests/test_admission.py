"""准入闸门：突发容量、立即拒绝、排队超时、取消、FIFO、热更新、恢复。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from governor.admission import AdmissionGate, AdmissionLimiter
from governor.config import Policy
from governor.errors import ClientCanceled, QueueRejected, QueueTimeout

log = logging.getLogger("test.admission")


class Holder:
    """持有准入槽位直到被释放的任务集合。"""

    def __init__(self, gate: AdmissionGate, n: int) -> None:
        self.gate = gate
        self.release = asyncio.Event()
        self.entered = [asyncio.Event() for _ in range(n)]
        self.tasks = [asyncio.create_task(self._hold(i)) for i in range(n)]

    async def _hold(self, i: int) -> None:
        async with self.gate.acquire():
            self.entered[i].set()
            await self.release.wait()

    async def wait_entered(self, n: int) -> None:
        for i in range(n):
            await asyncio.wait_for(self.entered[i].wait(), timeout=1)

    async def free(self) -> None:
        self.release.set()
        await asyncio.gather(*self.tasks)



async def test_burst_immediate_reject_and_recovery() -> None:
    # cap=2, queue=1：第 4 个请求立即拒绝
    gate = AdmissionGate(
        Policy(max_concurrency=2, max_queue=1, queue_timeout=1.0)
    )
    holders = Holder(gate, 2)
    await holders.wait_entered(2)

    waiter = asyncio.create_task(_enter(gate, "queued-1"))
    await asyncio.sleep(0.01)

    with pytest.raises(QueueRejected):
        async with gate.acquire():
            pass
    log.info("突发第 4 请求判定=capacity_full active=%d waiting=%d",
             gate.active, gate.waiting)

    # 压力解除：超时的排队者清掉后，新请求正常放行
    with pytest.raises(QueueTimeout):
        await waiter
    await holders.free()
    async with gate.acquire():
        log.info("压力解除后正常请求判定=admitted")
        assert gate.active == 1



async def test_queue_timeout_distinguishable() -> None:
    gate = AdmissionGate(
        Policy(max_concurrency=1, max_queue=2, queue_timeout=0.05)
    )
    holders = Holder(gate, 1)
    await holders.wait_entered(1)
    with pytest.raises(QueueTimeout):
        t0 = asyncio.get_running_loop().time()
        async with gate.acquire():
            pass
    elapsed = asyncio.get_running_loop().time() - t0
    assert elapsed >= 0.04
    log.info("排队超时判定=queue_timeout elapsed=%.3fs", elapsed)
    await holders.free()



async def test_client_cancel_does_not_leak_slot() -> None:
    gate = AdmissionGate(
        Policy(max_concurrency=1, max_queue=3, queue_timeout=5)
    )
    holders = Holder(gate, 1)
    await holders.wait_entered(1)

    canceled_task = asyncio.create_task(_enter(gate, "cancel-me"))
    await asyncio.sleep(0.02)
    canceled_task.cancel()
    with pytest.raises((ClientCanceled, asyncio.CancelledError)):
        await canceled_task

    await holders.free()
    # 取消者绝不能占用槽位：新请求立即成功
    async with gate.acquire():
        assert gate.active == 1
    log.info("取消清理判定=slot-released active=%d", gate.active)



async def test_fifo_order_and_capacity_upscale() -> None:
    gate = AdmissionGate(
        Policy(max_concurrency=1, max_queue=3, queue_timeout=5)
    )
    order: list[str] = []
    releases: dict[str, asyncio.Event] = {}

    async def seq(name: str, entered: asyncio.Event) -> None:
        async with gate.acquire():
            order.append(name)
            entered.set()
            await releases[name].wait()

    first_in = asyncio.Event()
    releases["1"] = asyncio.Event()
    t1 = asyncio.create_task(seq("1", first_in))
    await first_in.wait()

    evs = [asyncio.Event() for _ in range(3)]
    names = ["2", "3", "4"]
    for n in names:
        releases[n] = asyncio.Event()
    ts = [asyncio.create_task(seq(names[i], evs[i])) for i in range(3)]
    await asyncio.sleep(0.02)
    assert not any(e.is_set() for e in evs)

    # 容量上调到 3：严格按 FIFO 放行 2、3，4 继续等
    gate.update_policy(
        Policy(max_concurrency=3, max_queue=3, queue_timeout=5)
    )
    await asyncio.wait_for(
        asyncio.gather(evs[0].wait(), evs[1].wait()), timeout=1
    )
    assert not evs[2].is_set()
    assert order == ["1", "2", "3"]
    log.info("热更新上调判定=fifo-granted order=%s", order)

    # 释放 1 后 4 进入
    releases["1"].set()
    await t1
    await asyncio.wait_for(evs[2].wait(), timeout=1)
    assert order == ["1", "2", "3", "4"]
    for n in names:
        releases[n].set()
    await asyncio.gather(*ts)



async def test_limiter_keys_are_isolated() -> None:
    limiter = AdmissionLimiter()
    pol = Policy(max_concurrency=1, max_queue=0, queue_timeout=1)
    async with limiter.admit(("ep", "a"), pol):
        with pytest.raises(QueueRejected):
            async with limiter.admit(("ep", "a"), pol):
                pass
        # 不同调用方互不影响
        async with limiter.admit(("ep", "b"), pol):
            pass
    log.info("调用方维度隔离判定=independent-gates")


async def _enter(gate: AdmissionGate, name: str) -> None:
    async with gate.acquire():
        log.info("%s 判定=admitted", name)
