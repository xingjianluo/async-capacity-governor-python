"""运行时指标采集。

区分：
- 排队延迟 queue_wait_seconds 与处理/执行延迟 execute_seconds（含 total）；
- 拒绝原因 reject_reasons：capacity_full / queue_timeout；
- 实际执行量 executions（只统计真正调用后端的 leader 次数，合并者不算）；
- 合并命中 coalesced 与缓存命中 cache_hit。

按 total / endpoint:<名> / caller:<名> 三个维度聚合，写入在事件循环线程内
进行（单线程），快照读取做浅拷贝，避免计数漂移。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

# 拒绝原因常量（与 errors 中的 reason 保持一致）
REASON_CAPACITY_FULL = "capacity_full"
REASON_QUEUE_TIMEOUT = "queue_timeout"


@dataclass
class ScopeMetrics:
    started: int = 0
    success: int = 0
    rejected: int = 0
    degraded: int = 0
    failed: int = 0
    canceled: int = 0
    coalesced: int = 0
    cache_hit: int = 0
    executions: int = 0
    queue_timeouts: int = 0
    capacity_rejects: int = 0
    queue_wait_seconds: float = 0.0
    execute_seconds: float = 0.0
    total_seconds: float = 0.0
    reject_reasons: dict[str, int] = field(default_factory=dict)

    def add(self, **fields: object) -> None:
        for k, v in fields.items():
            cur = getattr(self, k)
            if isinstance(cur, dict):
                reason = str(v)
                cur[reason] = cur.get(reason, 0) + 1
            else:
                setattr(self, k, cur + v)  # type: ignore[operator]

    def as_dict(self) -> dict[str, object]:
        d = asdict(self)
        d["reject_reasons"] = dict(self.reject_reasons)
        return d


class MetricsRegistry:
    def __init__(self) -> None:
        self._scopes: dict[str, ScopeMetrics] = {}

    def _scope(self, name: str) -> ScopeMetrics:
        s = self._scopes.get(name)
        if s is None:
            s = ScopeMetrics()
            self._scopes[name] = s
        return s

    def record(
        self,
        endpoint: str | None,
        caller: str | None,
        **fields: object,
    ) -> None:
        for name in ("total", f"endpoint:{endpoint}", f"caller:{caller}"):
            self._scope(name).add(**fields)

    def observe_gate_states(self, states: dict[tuple[str, str], tuple[int, int]]) -> None:
        # 仅用于调试输出，不参与聚合计数
        self.gate_states = dict(states)

    def snapshot(self) -> dict[str, dict[str, object]]:
        return {name: s.as_dict() for name, s in self._scopes.items()}

    def reset(self) -> None:
        self._scopes.clear()
