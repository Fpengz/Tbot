"""Typed, exchange-neutral domain objects.

Monetary and asset quantities use Decimal. Values are normalized to the symbol's
exchange metadata before an execution adapter sends an order.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4


class TradingMode(StrEnum):
    BACKTEST = "backtest"
    PAPER = "paper"
    LIVE = "live"


class IntentSide(StrEnum):
    BUY = "buy"
    SELL = "sell"
    NO_TRADE = "no_trade"


class OrderStatus(StrEnum):
    PENDING = "pending"
    ACCEPTED = "accepted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class Trade:
    symbol: str
    price: Decimal
    size: Decimal
    side: IntentSide
    event_time: datetime
    trade_id: str

    def __post_init__(self) -> None:
        if self.side is IntentSide.NO_TRADE or min(self.price, self.size) <= 0:
            raise ValueError("trade needs a side and positive price/size")
        if self.event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")


@dataclass(frozen=True, slots=True)
class Bar:
    symbol: str
    start_time: datetime
    end_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int

    def __post_init__(self) -> None:
        if self.start_time.tzinfo is None or self.end_time.tzinfo is None:
            raise ValueError("bar timestamps must be timezone-aware")
        if self.end_time <= self.start_time or self.trade_count < 0 or self.volume < 0:
            raise ValueError("invalid bar interval, volume, or trade count")
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("bar prices must be positive")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("bar OHLC values are inconsistent")


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class BestBidAsk:
    symbol: str
    bid_price: Decimal
    bid_size: Decimal
    ask_price: Decimal
    ask_size: Decimal
    event_time: datetime

    def __post_init__(self) -> None:
        if self.event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")
        if min(self.bid_price, self.ask_price, self.bid_size, self.ask_size) <= 0:
            raise ValueError("book prices and sizes must be positive")
        if self.bid_price >= self.ask_price:
            raise ValueError("best bid must be below best ask")

    @property
    def mid_price(self) -> Decimal:
        return (self.bid_price + self.ask_price) / Decimal("2")


@dataclass(frozen=True, slots=True)
class OrderIntent:
    symbol: str
    side: IntentSide
    quantity: Decimal
    signal_time: datetime
    expires_at: datetime
    strategy_id: str
    correlation_id: UUID = field(default_factory=uuid4)
    score: Decimal | None = None
    rationale: str = ""

    def __post_init__(self) -> None:
        if self.signal_time.tzinfo is None or self.expires_at.tzinfo is None:
            raise ValueError("intent timestamps must be timezone-aware")
        if self.expires_at <= self.signal_time:
            raise ValueError("intent must expire after its signal time")
        if self.side is IntentSide.NO_TRADE:
            if self.quantity != 0:
                raise ValueError("no-trade intent must have zero quantity")
        elif self.quantity <= 0:
            raise ValueError("trade intent quantity must be positive")

    @classmethod
    def no_trade(
        cls, *, symbol: str, signal_time: datetime, strategy_id: str, rationale: str
    ) -> "OrderIntent":
        return cls(
            symbol=symbol,
            side=IntentSide.NO_TRADE,
            quantity=Decimal("0"),
            signal_time=signal_time,
            expires_at=signal_time + timedelta(minutes=5),
            strategy_id=strategy_id,
            rationale=rationale,
        )


@dataclass(frozen=True, slots=True)
class RiskDecision:
    intent: OrderIntent
    approved: bool
    reason: str
    decided_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class Fill:
    order_id: UUID
    symbol: str
    side: IntentSide
    quantity: Decimal
    price: Decimal
    fee: Decimal
    filled_at: datetime
    correlation_id: UUID

    def __post_init__(self) -> None:
        if self.side is IntentSide.NO_TRADE:
            raise ValueError("a fill cannot have no_trade side")
        if min(self.quantity, self.price) <= 0 or self.fee < 0:
            raise ValueError("fill quantity/price must be positive and fee non-negative")
        if self.filled_at.tzinfo is None:
            raise ValueError("filled_at must be timezone-aware")
