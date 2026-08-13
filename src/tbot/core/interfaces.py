"""Dependency-inversion contracts for live and simulated components."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from .models import Bar, BestBidAsk, Fill, OrderIntent, RiskDecision


class Strategy(Protocol):
    strategy_id: str

    def decide_from_bars(self, bars: list[Bar], decision_time: datetime) -> OrderIntent: ...


class RiskManager(Protocol):
    def assess(self, *, intent: OrderIntent, book: BestBidAsk, now: datetime) -> RiskDecision: ...


class ExecutionVenue(Protocol):
    def submit(self, *, decision: RiskDecision, book: BestBidAsk, now: datetime) -> Fill | None: ...


class Account(Protocol):
    cash_usd: Decimal
    btc: Decimal
    realized_pnl_usd: Decimal
    average_entry_price: Decimal

    @property
    def seen_fill_ids(self) -> frozenset[UUID]: ...

    def apply_fill(self, fill: Fill) -> bool: ...

    def restore_state(
        self,
        *,
        cash_usd: Decimal,
        btc: Decimal,
        realized_pnl_usd: Decimal,
        average_entry_price: Decimal,
        seen_fill_ids: Iterable[UUID],
    ) -> None: ...

    def equity(self, mark: Decimal) -> Decimal: ...
