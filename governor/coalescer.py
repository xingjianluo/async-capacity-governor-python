"""相同只读请求的短时合并（singleflight）与结果短期复用（TTL）。

设计要点：
- 首个请求（leader）负责执行 factory；并发的 follower 不重复执行，等待同一结果；
- leader 的执行不受任何客户端取消影响（asyncio.shield），follower 取消只让自己离开；
- leader 执行抛错则所有等待者收到同一异常，flight 立即清理，不缓存错误；
- follower 客户端取消抛 ClientCanceled，且不影响 leader 与其他 follower；
- 成功结果按 leader 策略的 result_ttl 缓存，过期后下一个请求成为新 leader；
- flight 的清理只由“最后一个离开者”（含 leader 自己）完成，避免计数漂移；
- 在结果产生前，任何等待者都不会提前拿到值；
- 主动作废（invalidate）：在途 flight 立即从注册表摘除并打作废标记，
  后端执行不被取消（避免连带副作用），但结果落地即丢弃、绝不进缓存；
  所有等待者收到 ResultInvalidated；已缓存未过期的 flight 直接摘除；
  后续请求成为新 leader 重新执行，不会被作废前的旧值污染；
- 作废是幂等无副作用的：重复作废或对不存在的 key 作废都安全返回 False。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass

from .errors import ClientCanceled, ResultInvalidated

Factory = Callable[[], object]


@dataclass
class _Flight:
    future: asyncio.Future[object]
    ttl: float
    result: object = None
    error: BaseException | None = None
    done: bool = False
    invalidated: bool = False  # 作废标记：结果落地即丢弃，等待者收 ResultInvalidated
    waiters: int = 0  # 含 leader，leader 以 1 起步
    finished_at: float = 0.0

    def fresh(self, now: float) -> bool:
        return (
            self.done
            and self.error is None
            and self.ttl > 0
            and (now - self.finished_at) < self.ttl
        )


class Coalescer:
    def __init__(self) -> None:
        self._flights: dict[str, _Flight] = {}

    async def run(
        self,
        key: str,
        factory: Factory,
        ttl: float,
        is_cancelled: Callable[[], bool] | None = None,
    ) -> tuple[object, bool, bool]:
        """执行（或等待合并、或读取缓存）一个只读请求。

        返回 (value, executed, cache_hit)：
        - executed=True：本次请求是 leader，实际调用了 factory；
        - cache_hit=True：未执行，命中 TTL 缓存；
        - 两者皆 False：未执行，合入在途请求（singleflight follower）。

        factory 为无参可调用对象，返回 awaitable，仅由 leader 调用一次。
        ttl 由发起执行的 leader 策略固定；后续 follower 不改写其语义。
        """
        loop = asyncio.get_running_loop()
        existing = self._flights.get(key)
        if existing is not None and existing.done and not existing.fresh(loop.time()):
            # 缓存过期/失败/不可缓存：丢弃旧 flight
            self._flights.pop(key, None)
            existing = None

        leader = existing is None
        cache_hit = existing is not None and existing.done
        if leader:
            try:
                aw = factory()
                fut = asyncio.ensure_future(aw)
            except BaseException:
                # factory 同步抛错：不留脏 flight
                raise
            fl = _Flight(future=fut, ttl=ttl, waiters=1)
            self._flights[key] = fl

            def _settle(task: asyncio.Future[object]) -> None:
                fl.done = True
                fl.finished_at = loop.time()
                if task.cancelled():
                    fl.error = asyncio.CancelledError()
                elif task.exception() is not None:
                    fl.error = task.exception()
                else:
                    fl.result = task.result()

            fut.add_done_callback(_settle)
        else:
            assert existing is not None
            fl = existing
            fl.waiters += 1

        try:
            # shield：任何一方取消都不取消共享执行
            await asyncio.shield(fl.future)
        except asyncio.CancelledError:
            # 仅表示“我”这个等待者放弃；leader 的执行继续
            raise ClientCanceled("客户端在等待合并结果期间取消") from None
        finally:
            fl.waiters -= 1
            should_remove = False
            if leader:
                # leader 离开：失败立即清理；成功但不可缓存也清理；
                # 成功且 ttl>0 时保留缓存（即使此时还有 follower 未走）
                if fl.done and (fl.error is not None or fl.ttl <= 0):
                    should_remove = True
            else:
                # follower 离开：若 flight 已终结且无人再等、且不该缓存则清理
                if fl.done and fl.waiters <= 0 and (fl.error is not None or fl.ttl <= 0):
                    should_remove = True
            if should_remove:
                # 仅当自己持有的还是当前 flight 时才弹，防 ABA
                if self._flights.get(key) is fl:
                    self._flights.pop(key, None)

        if fl.invalidated:
            # 共享结果已被作废：任何等待者（含 leader）都不得使用旧值
            raise ResultInvalidated("结果已被主动作废，需重新执行获取最新值")
        if is_cancelled is not None and is_cancelled():
            raise ClientCanceled("结果就绪后调用方已取消")
        if fl.error is not None:
            raise fl.error
        return fl.result, leader, cache_hit

    def invalidate(self, key: str) -> bool:
        """主动作废某 key 的只读结果（在途合并执行 / 已缓存未过期）。

        - 在途：flight 立即从注册表摘除并打作废标记；后端执行不被取消
          （跑完后结果直接丢弃、不进缓存），所有等待者收 ResultInvalidated；
        - 已缓存：直接摘除，后续请求成为新 leader 重新执行；
        - 不存在 / 重复作废：安全无操作，返回 False。

        返回 True 表示本次调用实际作废了一个结果。
        """
        fl = self._flights.pop(key, None)
        if fl is None:
            return False
        fl.invalidated = True
        return True

    def flight_count(self) -> int:
        return len(self._flights)
