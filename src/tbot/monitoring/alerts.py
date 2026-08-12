"""Alert routing with deduplication to prevent outage alert storms."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True, slots=True)
class Alert:
    severity: str
    code: str
    message: str
    created_at: datetime


class AlertManager:
    def __init__(self, deliver: Callable[[Alert], None], *, cooldown_seconds: int = 60) -> None:
        self.deliver, self.cooldown = deliver, timedelta(seconds=cooldown_seconds)
        self._last_sent: dict[str, datetime] = {}

    def emit(self, severity: str, code: str, message: str, *, now: datetime | None = None) -> bool:
        now = now or datetime.now(UTC)
        last_sent = self._last_sent.get(code)
        if last_sent is not None and now - last_sent < self.cooldown:
            return False
        self._last_sent[code] = now
        self.deliver(Alert(severity, code, message, now))
        return True

    def stale_data(self, age_seconds: float) -> None:
        self.emit("critical", "stale_data", f"market data is {age_seconds:.1f}s old")

    def reconciliation_mismatch(self, message: str) -> None:
        self.emit("critical", "reconciliation_mismatch", message)

    def unexpected_order(self, order_id: str) -> None:
        self.emit("critical", "unexpected_order", f"unexpected order: {order_id}")
