"""Data-quality state. Consumers must check it before placing a decision."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class FeedHealth:
    last_event_time: datetime | None = None
    invalid_messages: int = 0
    duplicate_events: int = 0
    gaps_detected: int = 0
    _last_sequence: int | None = None

    def record_event(self, event_time: datetime) -> None:
        if event_time.tzinfo is None:
            raise ValueError("event time must be timezone-aware")
        self.last_event_time = event_time

    def is_fresh(self, *, now: datetime, max_age_seconds: int) -> bool:
        if self.last_event_time is None:
            return False
        age = (now - self.last_event_time).total_seconds()
        return 0 <= age <= max_age_seconds

    def observe_sequence(self, sequence: int) -> bool:
        """Return whether the next exchange sequence is contiguous; never hide gaps."""
        if self._last_sequence is not None and sequence <= self._last_sequence:
            self.duplicate_events += 1
            return False
        contiguous = self._last_sequence is None or sequence == self._last_sequence + 1
        if not contiguous:
            self.gaps_detected += 1
        self._last_sequence = sequence
        return contiguous

    def reset_sequence(self, sequence: int | None = None) -> None:
        """A fresh book snapshot establishes a new valid sequence baseline."""
        self._last_sequence = sequence
