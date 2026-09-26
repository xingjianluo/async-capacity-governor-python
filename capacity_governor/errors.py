"""Typed errors and rejection reasons for the capacity governor."""

from __future__ import annotations

import enum


class RejectReason(enum.Enum):
    """Why a request was not admitted."""

    QUEUE_FULL = "queue_full"
    QUEUE_TIMEOUT = "queue_timeout"
    CANCELLED = "cancelled"
    CONCURRENCY_CONFLICT = "concurrency_conflict"


class AdmissionRejected(Exception):
    """Raised when a request is refused admission (capacity exceeded)."""

    def __init__(self, reason: RejectReason, detail: str = "") -> None:
        super().__init__(detail or reason.value)
        self.reason = reason


class QueueTimeout(AdmissionRejected):
    """Raised when a request waited in queue longer than allowed."""

    def __init__(self, detail: str = "") -> None:
        super().__init__(RejectReason.QUEUE_TIMEOUT, detail)


class RequestCancelled(AdmissionRejected):
    """Raised when the caller cancelled while queued or executing."""

    def __init__(self, detail: str = "") -> None:
        super().__init__(RejectReason.CANCELLED, detail)
