"""Dependency-inversion contracts for live and simulated components."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from .models import BestBidAsk, OrderIntent, RiskDecision


class Strategy(Protocol):
    strategy_id: str

    def decide(self, *, decision_time: datetime, book: BestBidAsk) -> OrderIntent: ...


class RiskManager(Protocol):
    def assess(self, *, intent: OrderIntent, book: BestBidAsk) -> RiskDecision: ...


class ExecutionVenue(Protocol):
    def submit(self, *, decision: RiskDecision, book: BestBidAsk) -> None: ...

