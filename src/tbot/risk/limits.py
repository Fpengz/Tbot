from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from tbot.core.models import BestBidAsk, IntentSide, OrderIntent, RiskDecision
from tbot.portfolio.account import PaperAccount

@dataclass(frozen=True, slots=True)
class RiskLimits:
    max_position_btc: Decimal
    max_order_notional_usd: Decimal
    max_daily_loss_usd: Decimal
    data_stale_after_seconds: int = 10

class BasicRiskManager:
    def __init__(self, account: PaperAccount, limits: RiskLimits, *, kill_switch: bool = False) -> None:
        self.account, self.limits, self.kill_switch = account, limits, kill_switch
    def assess(self, *, intent: OrderIntent, book: BestBidAsk, now: datetime) -> RiskDecision:
        if self.kill_switch: return RiskDecision(intent, False, "kill switch enabled", now)
        if intent.side is IntentSide.NO_TRADE: return RiskDecision(intent, False, "no trade", now)
        if now > intent.expires_at: return RiskDecision(intent, False, "intent expired", now)
        if (now - book.event_time).total_seconds() > self.limits.data_stale_after_seconds:
            return RiskDecision(intent, False, "stale market data", now)
        notional = intent.quantity * (book.ask_price if intent.side is IntentSide.BUY else book.bid_price)
        if notional > self.limits.max_order_notional_usd: return RiskDecision(intent, False, "order notional limit", now)
        projected = self.account.btc + (intent.quantity if intent.side is IntentSide.BUY else -intent.quantity)
        if abs(projected) > self.limits.max_position_btc: return RiskDecision(intent, False, "position limit", now)
        if intent.side is IntentSide.BUY and notional > self.account.cash_usd: return RiskDecision(intent, False, "insufficient cash", now)
        if intent.side is IntentSide.SELL and intent.quantity > self.account.btc: return RiskDecision(intent, False, "spot account has insufficient BTC", now)
        if self.account.realized_pnl_usd <= -self.limits.max_daily_loss_usd: return RiskDecision(intent, False, "daily loss limit", now)
        return RiskDecision(intent, True, "approved", now)
