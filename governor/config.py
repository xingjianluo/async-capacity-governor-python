"""容量策略配置与运行时动态配置存储。

配置以不可变快照（Snapshot）形式整体替换：在途请求始终持有其准入时刻
计算出的 Policy，配置更新只影响之后的新请求，不会改变在途请求的语义。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

WILDCARD_CALLER = "*"


@dataclass(frozen=True)
class Policy:
    """单个接口（可按调用方覆盖）的容量策略。

    max_concurrency: 实际并发执行上限。
    max_queue:       上限占满后允许排队的请求数（0 表示不排队，立即拒绝）。
    queue_timeout:   排队最长等待秒数，超时返回 queue_timeout。
    coalesce_window: >0 时开启相同只读请求合并（singleflight 等待窗口提示）。
    result_ttl:      成功结果复用秒数；<=0 表示不复用。
    fallback_value:  非 None 表示容量受限时允许降级返回该值；None 表示不降级。
    """

    max_concurrency: int = 8
    max_queue: int = 16
    queue_timeout: float = 1.0
    coalesce_window: float = 0.0
    result_ttl: float = 0.0
    fallback_value: object = None

    def __post_init__(self) -> None:
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency 必须 >= 1")
        if self.max_queue < 0:
            raise ValueError("max_queue 必须 >= 0")
        if self.queue_timeout < 0:
            raise ValueError("queue_timeout 必须 >= 0")
        if self.coalesce_window < 0:
            raise ValueError("coalesce_window 必须 >= 0")
        if self.result_ttl < 0:
            raise ValueError("result_ttl 必须 >= 0")

    @property
    def coalesce_enabled(self) -> bool:
        return self.coalesce_window > 0

    @property
    def degrade_enabled(self) -> bool:
        return self.fallback_value is not None


@dataclass(frozen=True)
class Snapshot:
    """某一时刻的完整配置快照。

    policies 的 key 为 (endpoint, caller)；caller 可为 "*" 表示该接口默认策略。
    """

    default: Policy = field(default_factory=Policy)
    policies: dict[tuple[str, str], Policy] = field(default_factory=dict)


class ConfigStore:
    """线程/任务安全的只读快照配置存储，支持原子替换。"""

    def __init__(self, snapshot: Snapshot | None = None) -> None:
        self._lock = threading.Lock()
        self._snapshot = snapshot or Snapshot()

    def get(self) -> Snapshot:
        with self._lock:
            return self._snapshot

    def replace(self, snapshot: Snapshot) -> None:
        """整体原子替换；在途请求持有的旧快照不受影响。"""
        with self._lock:
            self._snapshot = snapshot

    def policy_for(self, endpoint: str, caller: str) -> Policy:
        snap = self.get()
        exact = snap.policies.get((endpoint, caller))
        if exact is not None:
            return exact
        wildcard = snap.policies.get((endpoint, WILDCARD_CALLER))
        if wildcard is not None:
            return wildcard
        return snap.default
