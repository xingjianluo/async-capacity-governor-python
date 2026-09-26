"""请求容量治理与热点观测包。"""

from .errors import (
    ClientCanceled,
    Degraded,
    GovernorError,
    QueueRejected,
    QueueTimeout,
)

__all__ = [
    "GovernorError",
    "QueueRejected",
    "QueueTimeout",
    "ClientCanceled",
    "Degraded",
]
