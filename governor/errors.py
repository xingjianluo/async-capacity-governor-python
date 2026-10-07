"""容量治理相关异常，映射为可区分的 HTTP 响应。"""

from __future__ import annotations


class GovernorError(Exception):
    """治理层异常基类。"""

    status_code: int = 500
    error_code: str = "governor_error"
    reason: str = "governor_error"


class QueueRejected(GovernorError):
    """并发与排队容量均已占满，立即拒绝。"""

    status_code = 429
    error_code = "capacity_rejected"
    reason = "capacity_full"


class QueueTimeout(GovernorError):
    """排队等待超过最大等待时间。"""

    status_code = 503
    error_code = "queue_timeout"
    reason = "queue_timeout"


class ClientCanceled(GovernorError):
    """客户端在排队或执行期间主动取消。"""

    status_code = 499
    error_code = "client_canceled"
    reason = "client_canceled"


class ResultInvalidated(GovernorError):
    """等待中的共享结果已被主动作废（强制刷新），调用方不得使用旧值。"""

    status_code = 409
    error_code = "result_invalidated"
    reason = "invalidated"


class Degraded(GovernorError):
    """内部信号：命中降级分支（不作为 HTTP 异常抛出）。"""

    status_code = 503
    error_code = "degraded"
    reason = "degraded"

    def __init__(self, cause: Exception, payload: object):
        super().__init__(str(cause))
        self.cause = cause
        self.payload = payload
