"""按 (接口, 调用方) 维度的并发准入与有界排队。

语义：
- 并发低于 max_concurrency 且队列空闲：立即放行；
- 否则进入 FIFO 队列，队列长度达到 max_queue 时立即抛 QueueRejected；
- 排队超过 queue_timeout 抛 QueueTimeout；
- 排队期间客户端取消抛 ClientCanceled；
- 严格 FIFO 放行，容量上调时也不会让后来者插队；
- 容量下调不抢占在途请求，仅让闸门自然收敛到新上限。
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from collections.abc import AsyncIterator
from typing import Any

from .config import Policy
from .errors import ClientCanceled, QueueRejected, QueueTimeout


class AdmissionGate:
    """单个 (endpoint, caller) 维度的准入闸门。"""

    def __init__(self, policy: Policy) -> None:
        self._policy = policy
        self._cap = policy.max_concurrency
        self._active = 0
        self._queue: deque[asyncio.Future[None]] = deque()

    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return len(self._queue)

    def snapshot_state(self) -> tuple[int, int]:
        return self._active, len(self._queue)

    def update_policy(self, policy: Policy) -> None:
        """热更新策略。容量上调时立即补放排队者；下调不抢占。"""
        self._policy = policy
        if policy.max_concurrency > self._cap:
            self._cap = policy.max_concurrency
            self._pump()
        else:
            self._cap = policy.max_concurrency

    @contextlib.asynccontextmanager
    async def acquire(self) -> AsyncIterator[None]:
        waiter: asyncio.Future[None] | None = None
        slot = False
        try:
            if self._active < self._cap and not self._queue:
                # 快速路径
                self._active += 1
                slot = True
            else:
                if len(self._queue) >= self._policy.max_queue:
                    raise QueueRejected(
                        f"active={self._active}, queue={len(self._queue)}, "
                        f"cap={self._cap}, max_queue={self._policy.max_queue}"
                    )
                waiter = asyncio.get_running_loop().create_future()
                self._queue.append(waiter)
                try:
                    # shield 保护闸门内部的 waiter，不被外部取消直接 cancel；
                    # 外部取消时我们自行把未授权的 waiter 移出队列。
                    await asyncio.wait_for(
                        asyncio.shield(waiter),
                        timeout=self._policy.queue_timeout,
                    )
                except asyncio.TimeoutError:
                    raise QueueTimeout(
                        f"排队超过 {self._policy.queue_timeout}s"
                    ) from None
                except asyncio.CancelledError:
                    # 极端竞态：取消与授权同一事件循环 tick 到达，
                    # 已授权则按持槽处理，在 finally 中正常释放。
                    if waiter.done() and not waiter.cancelled():
                        slot = True
                    raise ClientCanceled("客户端在排队期间取消") from None
                else:
                    slot = True
            yield
        finally:
            if slot:
                self._active -= 1
                self._pump()
            elif waiter is not None and waiter in self._queue:
                self._queue.remove(waiter)

    def _pump(self) -> None:
        """按 FIFO 把空闲容量授予队首等待者。"""
        while self._active < self._cap and self._queue:
            waiter = self._queue.popleft()
            if waiter.done():
                continue
            self._active += 1
            waiter.set_result(None)


class AdmissionLimiter:
    """按 key 管理全部闸门；同一事件循环内使用，无需重锁。"""

    def __init__(self) -> None:
        self._gates: dict[tuple[str, str], AdmissionGate] = {}

    def gate(self, key: tuple[str, str], policy: Policy) -> AdmissionGate:
        g = self._gates.get(key)
        if g is None:
            g = AdmissionGate(policy)
            self._gates[key] = g
            return g
        # 每次请求都把最新策略下发给闸门（容量上调会立即补放）。
        g.update_policy(policy)
        return g

    def states(self) -> dict[tuple[str, str], tuple[int, int]]:
        return {k: g.snapshot_state() for k, g in self._gates.items()}

    @contextlib.asynccontextmanager
    async def admit(
        self, key: tuple[str, str], policy: Policy
    ) -> AsyncIterator[None]:
        async with self.gate(key, policy).acquire():
            yield


# 兼容可能的 Any 类型引用
__all__: list[Any] = ["AdmissionGate", "AdmissionLimiter"]
