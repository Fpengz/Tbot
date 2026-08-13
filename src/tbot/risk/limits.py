from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_CEILING, Decimal

from tbot.core.interfaces import Account
from tbot.core.models import BestBidAsk, IntentSide, OrderIntent, RiskDecision


@dataclass(frozen=True, slots=True)
class RiskLimits:
    max_position_btc: Decimal
    max_order_notional_usd: Decimal
    max_daily_loss_usd: Decimal
    data_stale_after_seconds: int = 10
    taker_fee_bps: Decimal = Decimal(0)
    slippage_bps: Decimal = Decimal(0)
    price_increment: Decimal = Decimal("0.01")

    def __post_init__(self) -> None:
        if self.data_stale_after_seconds < 0:
            raise ValueError("data staleness threshold must not be negative")
        if self.taker_fee_bps < 0 or self.slippage_bps < 0:
            raise ValueError("execution costs must not be negative")
        if self.price_increment <= 0:
            raise ValueError("price increment must be positive")

    def estimated_buy_cash(self, *, quantity: Decimal, ask_price: Decimal) -> Decimal:
        """Return a conservative cash requirement for the simulator's buy fill."""
        slippage_multiplier = Decimal(1) + self.slippage_bps / Decimal(10000)
        adjusted_price = ask_price * slippage_multiplier
        executable_price = (adjusted_price / self.price_increment).to_integral_value(
            rounding=ROUND_CEILING
        ) * self.price_increment
        fee_multiplier = Decimal(1) + self.taker_fee_bps / Decimal(10000)
        return quantity * executable_price * fee_multiplier


class BasicRiskManager:
    def __init__(self, account: Account, limits: RiskLimits, *, kill_switch: bool = False) -> None:
        self.account, self.limits, self.kill_switch = account, limits, kill_switch

    def assess(self, *, intent: OrderIntent, book: BestBidAsk, now: datetime) -> RiskDecision:
        if self.kill_switch:
            return RiskDecision(intent, False, "kill switch enabled", now)
        if intent.side is IntentSide.NO_TRADE:
            return RiskDecision(intent, False, "no trade", now)
        if now > intent.expires_at:
            return RiskDecision(intent, False, "intent expired", now)
        if intent.symbol != book.symbol:
            return RiskDecision(intent, False, "symbol mismatch", now)
        book_age = (now - book.event_time).total_seconds()
        if book_age < 0 or book_age > self.limits.data_stale_after_seconds:
            return RiskDecision(intent, False, "stale market data", now)
        notional = intent.quantity * (
            book.ask_price if intent.side is IntentSide.BUY else book.bid_price
        )
        if notional > self.limits.max_order_notional_usd:
            return RiskDecision(intent, False, "order notional limit", now)
        projected = self.account.btc + (
            intent.quantity if intent.side is IntentSide.BUY else -intent.quantity
        )
        if abs(projected) > self.limits.max_position_btc:
            return RiskDecision(intent, False, "position limit", now)
        if (
            intent.side is IntentSide.BUY
            and self.limits.estimated_buy_cash(quantity=intent.quantity, ask_price=book.ask_price)
            > self.account.cash_usd
        ):
            return RiskDecision(intent, False, "insufficient cash including costs", now)
        if intent.side is IntentSide.SELL and intent.quantity > self.account.btc:
            return RiskDecision(intent, False, "spot account has insufficient BTC", now)
        if self.account.realized_pnl_usd <= -self.limits.max_daily_loss_usd:
            return RiskDecision(intent, False, "daily loss limit", now)
        return RiskDecision(intent, True, "approved", now)
