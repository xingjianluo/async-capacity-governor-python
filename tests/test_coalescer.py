"""singleflight 合并 + TTL 复用：
结果未产生不得提前返回；失败/取消不污染后续；leader 取消不杀执行。"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from governor.coalescer import Coalescer
from governor.errors import ClientCanceled

log = logging.getLogger("test.coalesce")


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


async def test_burst_merged_into_single_execution() -> None:
    c = Coalescer()
    counter = Counter()
    results = await asyncio.gather(*(
        c.run("k", counter.factory(delay=0.05), ttl=1.0) for _ in range(5)
    ))
    assert [r[0] for r in results] == ["v"] * 5
    assert sum(1 for r in results if r[1]) == 1
    assert counter.n == 1
    log.info("突发合并判定=1-execution-for-5 requests")

    # 结果未产生时 follower 绝不能提前拿到值
    c2 = Coalescer()
    ctr = Counter()
    task = asyncio.create_task(c2.run("x", ctr.factory(delay=0.2), 1.0))
    await ctr.started.wait()
    follower = asyncio.create_task(
        c2.run("x", ctr.factory(delay=0.2), 1.0)
    )
    await asyncio.sleep(0.02)
    assert not follower.done(), "结果未产生时不得提前返回"
    assert (await follower)[0] == "v"
    assert (await task)[0] == "v"
    log.info("在途等待判定=no-early-return")


async def test_ttl_cache_hit_and_expiry() -> None:
    c = Coalescer()
    counter = Counter()
    first = await c.run("k", counter.factory(delay=0.01), ttl=0.1)
    assert first == ("v", True, False)
    assert counter.n == 1
    # TTL 内：不执行
    hit = await c.run("k", counter.factory(delay=0.01), ttl=0.1)
    assert hit == ("v", False, True)
    assert counter.n == 1
    log.info("TTL 内判定=cache-hit executions=%d", counter.n)
    # 过期：重新执行
    await asyncio.sleep(0.12)
    again = await c.run("k", counter.factory(delay=0.01), ttl=0.1)
    assert again[0] == "v" and again[1] is True
    assert counter.n == 2
    log.info("TTL 过期判定=re-execute executions=%d", counter.n)
    # ttl=0：永不缓存
    r1 = await c.run("z", counter.factory(delay=0.0), ttl=0.0)
    r2 = await c.run("z", counter.factory(delay=0.0), ttl=0.0)
    assert r1[1] is True and r2[1] is True
    assert counter.n == 4


async def test_failure_does_not_pollute_later_requests() -> None:
    c = Coalescer()

    async def boom() -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("backend down")

    good = Counter()
    with contextlib.suppress(RuntimeError):
        await asyncio.gather(
            c.run("k", boom, ttl=1.0), c.run("k", boom, ttl=1.0)
        )
    assert c.flight_count() == 0
    result = await c.run("k", good.factory(result="ok", delay=0.0), 1.0)
    assert result[0] == "ok" and result[1] is True
    assert good.n == 1
    log.info("失败后判定=flight-cleaned retry-ok executions=%d", good.n)


async def test_follower_cancel_isolated_and_mapped() -> None:
    c = Coalescer()
    counter = Counter()
    t1 = asyncio.create_task(c.run("k", counter.factory(delay=0.15), 1.0))
    t2 = asyncio.create_task(c.run("k", counter.factory(delay=0.15), 1.0))
    t3 = asyncio.create_task(c.run("k", counter.factory(delay=0.15), 1.0))
    await counter.started.wait()
    t1.cancel()
    with contextlib.suppress(ClientCanceled, asyncio.CancelledError):
        await t1
    # leader 执行不受影响，其他 follower 正常收到结果
    assert (await t2)[0] == "v"
    assert (await t3)[0] == "v"
    assert counter.n == 1
    log.info("follower 取消判定=isolated executions=%d", counter.n)


async def test_leader_cancel_keeps_execution_running() -> None:
    c = Coalescer()
    counter = Counter()
    leader = asyncio.create_task(c.run("k", counter.factory(delay=0.1), 1.0))
    follower = asyncio.create_task(
        c.run("k", counter.factory(delay=0.1), 1.0)
    )
    await counter.started.wait()
    leader.cancel()
    with contextlib.suppress(ClientCanceled, asyncio.CancelledError):
        await leader
    assert (await follower)[0] == "v"
    assert counter.n == 1
    log.info("leader 取消判定=execution-continues executions=%d", counter.n)
