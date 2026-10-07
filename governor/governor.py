"""编排层：准入 -> 合并/复用 -> 执行 -> 降级 -> 指标与结构化日志。

每个请求持有准入时刻计算出的不可变 Policy；配置热更新只影响后续请求，
在途请求（排队中或执行中）始终按旧策略走完，不会出现旧配置语义突变。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .admission import AdmissionLimiter
from .coalescer import Coalescer
from .config import ConfigStore, Policy, Snapshot, WILDCARD_CALLER
from .errors import (
    ClientCanceled,
    GovernorError,
    QueueRejected,
    QueueTimeout,
    ResultInvalidated,
)
from .metrics import (
    REASON_CAPACITY_FULL,
    REASON_QUEUE_TIMEOUT,
    MetricsRegistry,
)

logger = logging.getLogger("governor")

WorkFactory = Callable[[], Any]

STATUS_SUCCESS = "success"            # 实际执行后成功
STATUS_CACHE = "success_cache"        # 命中 TTL 缓存，未执行
STATUS_COALESCED = "success_coalesced"  # 合入在途请求，未执行
STATUS_DEGRADED = "degraded"          # 容量拒绝/执行失败后降级
STATUS_REJECTED = "rejected"          # 容量拒绝（不降级）
STATUS_FAILED = "failed"              # 执行失败（不降级）
STATUS_CANCELED = "canceled"          # 客户端取消
STATUS_INVALIDATED = "invalidated"    # 等待中的共享结果被主动作废


@dataclass
class Outcome:
    status: str
    value: object = None
    cause: Exception | None = None
    queue_wait: float = 0.0
    execute_time: float = 0.0
    total_time: float = 0.0
    executed: bool = False
    reject_reason: str | None = None


_COUNTER_MAP = {
    STATUS_SUCCESS: "success",
    STATUS_CACHE: "success",
    STATUS_COALESCED: "success",
    STATUS_DEGRADED: "degraded",
    STATUS_REJECTED: "rejected",
    STATUS_FAILED: "failed",
    STATUS_CANCELED: "canceled",
    STATUS_INVALIDATED: "invalidated",
}


def _reason_of(exc: Exception) -> str:
    if isinstance(exc, QueueTimeout):
        return REASON_QUEUE_TIMEOUT
    if isinstance(exc, QueueRejected):
        return REASON_CAPACITY_FULL
    return getattr(exc, "reason", "rejected")


class Governor:
    def __init__(
        self,
        config: ConfigStore,
        limiter: AdmissionLimiter | None = None,
        coalescer: Coalescer | None = None,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        self.config = config
        self.limiter = limiter or AdmissionLimiter()
        self.coalescer = coalescer or Coalescer()
        self.metrics = metrics or MetricsRegistry()

    def reload(self) -> None:
        """把当前快照的策略下发给已存在的闸门（容量上调立即补放排队者）。"""
        snap = self.config.get()
        for (ep, caller), gate in self.limiter._gates.items():  # noqa: SLF001
            gate.update_policy(_resolve(snap, ep, caller))

    def invalidate(
        self,
        coalesce_key: str,
        endpoint: str | None = None,
        caller: str | None = None,
    ) -> bool:
        """主动作废某个合并 key 的在途/缓存只读结果（强制刷新）。

        返回是否命中现存结果；对不存在/已作废的 key 重复调用安全无副作用。
        等待中的调用方会收到可区分的 invalidated 结果，后续请求重新执行。
        """
        hit = self.coalescer.invalidate(coalesce_key)
        self.metrics.record(endpoint, caller, invalidations=1)
        logger.info(
            "invalidate key=%s endpoint=%s caller=%s hit=%s",
            coalesce_key, endpoint, caller, hit,
        )
        return hit

    async def handle(
        self,
        endpoint: str,
        caller: str,
        request_id: str,
        factory: WorkFactory,
        coalesce_key: str | None = None,
    ) -> Outcome:
        t0 = time.perf_counter()
        # 准入时刻快照策略：本请求全程使用，不受后续配置更新影响
        policy = self.config.policy_for(endpoint, caller)
        self.metrics.record(endpoint, caller, started=1)
        queue_wait = 0.0
        executed = False

        def emit(out: Outcome) -> Outcome:
            fields: dict[str, object] = {
                _COUNTER_MAP[out.status]: 1,
                "total_seconds": out.total_time,
            }
            if out.queue_wait:
                fields["queue_wait_seconds"] = out.queue_wait
            if out.execute_time:
                fields["execute_seconds"] = out.execute_time
            if out.executed:
                fields["executions"] = 1
            if out.status == STATUS_COALESCED:
                fields["coalesced"] = 1
            if out.status == STATUS_CACHE:
                fields["cache_hit"] = 1
            if out.reject_reason == REASON_QUEUE_TIMEOUT:
                fields["queue_timeouts"] = 1
            if out.reject_reason == REASON_CAPACITY_FULL:
                fields["capacity_rejects"] = 1
            if out.reject_reason is not None:
                fields["reject_reasons"] = out.reject_reason
            self.metrics.record(endpoint, caller, **fields)
            logger.info(
                "req=%s endpoint=%s caller=%s decision=%s%s queued=%.4fs "
                "execute=%.4fs total=%.4fs executed=%s value_type=%s",
                request_id, endpoint, caller, out.status,
                f" reason={out.reject_reason}" if out.reject_reason else "",
                out.queue_wait, out.execute_time, out.total_time, out.executed,
                type(out.value).__name__ if out.value is not None else "-",
            )
            return out

        def make_outcome(
            status: str,
            *,
            value: object = None,
            cause: Exception | None = None,
            execute_time: float = 0.0,
            reject_reason: str | None = None,
        ) -> Outcome:
            return Outcome(
                status=status,
                value=value,
                cause=cause,
                queue_wait=queue_wait,
                execute_time=execute_time,
                total_time=time.perf_counter() - t0,
                executed=executed,
                reject_reason=reject_reason,
            )

        try:
            # 1) 准入（含有界排队）
            q_start = time.perf_counter()
            key = (endpoint, caller)
            async with self.limiter.admit(key, policy):
                queue_wait = time.perf_counter() - q_start

                # 2) 合并 / 复用 / 执行
                exec_start = time.perf_counter()

                async def guarded() -> object:
                    nonlocal executed
                    executed = True
                    return await factory()

                try:
                    if policy.coalesce_enabled and coalesce_key is not None:
                        value, did_execute, cache_hit = await self.coalescer.run(
                            coalesce_key, guarded, ttl=policy.result_ttl
                        )
                        executed = did_execute
                        execute_time = time.perf_counter() - exec_start
                        if did_execute:
                            return emit(make_outcome(STATUS_SUCCESS, value=value,
                                                    execute_time=execute_time))
                        status = STATUS_CACHE if cache_hit else STATUS_COALESCED
                        return emit(make_outcome(status, value=value,
                                                execute_time=execute_time))

                    value = await guarded()
                    execute_time = time.perf_counter() - exec_start
                    return emit(make_outcome(STATUS_SUCCESS, value=value,
                                            execute_time=execute_time))
                except GovernorError:
                    raise
                except asyncio.CancelledError:
                    raise ClientCanceled("客户端在执行期间取消") from None
                except Exception as exc:  # noqa: BLE001
                    execute_time = time.perf_counter() - exec_start
                    if policy.degrade_enabled:
                        return emit(make_outcome(
                            STATUS_DEGRADED, value=policy.fallback_value,
                            cause=exc, execute_time=execute_time))
                    return emit(make_outcome(
                        STATUS_FAILED, cause=exc, execute_time=execute_time))
        except GovernorError as exc:
            if isinstance(exc, ResultInvalidated):
                # 等待中的共享结果被判作废：可区分状态，不降级、不算拒绝；
                # 准入槽位已随上下文退出正常释放，无计数漂移。
                return emit(make_outcome(STATUS_INVALIDATED, cause=exc))
            if isinstance(exc, ClientCanceled):
                out = make_outcome(STATUS_CANCELED, cause=exc)
                # 取消也记录总耗时（若已在执行，排队耗时照常统计）
                self.metrics.record(
                    endpoint, caller, canceled=1,
                    total_seconds=out.total_time,
                    **({"queue_wait_seconds": queue_wait} if queue_wait else {}),
                )
                logger.info(
                    "req=%s endpoint=%s caller=%s decision=canceled "
                    "queued=%.4fs total=%.4fs executed=%s",
                    request_id, endpoint, caller,
                    queue_wait, out.total_time, executed,
                )
                return out
            # QueueRejected / QueueTimeout
            out = make_outcome(
                STATUS_DEGRADED if policy.degrade_enabled else STATUS_REJECTED,
                value=policy.fallback_value if policy.degrade_enabled else None,
                cause=exc,
                reject_reason=_reason_of(exc),
            )
            return emit(out)


def _resolve(snap: Snapshot, ep: str, caller: str) -> Policy:
    pol = snap.policies.get((ep, caller))
    if pol is None:
        pol = snap.policies.get((ep, WILDCARD_CALLER))
    return pol if pol is not None else snap.default
