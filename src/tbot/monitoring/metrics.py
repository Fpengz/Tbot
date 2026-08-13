"""Thread-safe in-process metrics with Prometheus text exposition."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from threading import Lock
from typing import Any


def _metric_name(name: str) -> str:
    return "tbot_" + "".join(character if character.isalnum() else "_" for character in name)


@dataclass(slots=True)
class Metrics:
    counters: Counter[str] = field(default_factory=Counter)
    gauges: dict[str, Decimal] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock, repr=False)

    def increment(self, name: str, value: int = 1) -> None:
        if value < 0:
            raise ValueError("counter increments must be non-negative")
        with self._lock:
            self.counters[name] += value

    def set_gauge(self, name: str, value: Decimal) -> None:
        with self._lock:
            self.gauges[name] = value

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "timestamp": datetime.now(UTC).isoformat(),
                "counters": dict(self.counters),
                "gauges": {key: str(value) for key, value in self.gauges.items()},
            }

    def prometheus(self) -> str:
        snapshot = self.snapshot()
        lines = ["# tbot in-process observability metrics"]
        for name, value in sorted(snapshot["counters"].items()):
            lines.append(f"{_metric_name(name)}_total {value}")
        for name, value in sorted(snapshot["gauges"].items()):
            lines.append(f"{_metric_name(name)} {value}")
        return "\n".join(lines) + "\n"
