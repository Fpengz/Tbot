from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from uuid import uuid4
from tbot.core.models import BestBidAsk, Fill, IntentSide, RiskDecision

@dataclass(frozen=True, slots=True)
class SimulatorCosts:
    taker_fee_bps: Decimal
    slippage_bps: Decimal

@dataclass(frozen=True, slots=True)
class SymbolRules:
    price_increment: Decimal = Decimal("0.01")
    quantity_increment: Decimal = Decimal("0.00000001")
    minimum_notional_usd: Decimal = Decimal("1")
    def validate(self, *, quantity: Decimal, price: Decimal) -> None:
        if quantity % self.quantity_increment != 0: raise ValueError("quantity violates exchange increment")
        if price % self.price_increment != 0: raise ValueError("price violates exchange increment")
        if quantity * price < self.minimum_notional_usd: raise ValueError("order below minimum notional")
    def executable_price(self, raw_price: Decimal, side: IntentSide) -> Decimal:
        """Round adversely, just as a marketable order must respect a tick size."""
        rounding = ROUND_CEILING if side is IntentSide.BUY else ROUND_FLOOR
        return (raw_price / self.price_increment).to_integral_value(rounding=rounding) * self.price_increment

class SimulatedVenue:
    """Fill at best opposite quote plus configurable adverse slippage."""
    def __init__(self, costs: SimulatorCosts, rules: SymbolRules = SymbolRules()) -> None: self.costs, self.rules = costs, rules
    def submit(self, *, decision: RiskDecision, book: BestBidAsk, now: datetime) -> Fill | None:
        if not decision.approved: return None
        intent = decision.intent
        raw_price = book.ask_price if intent.side is IntentSide.BUY else book.bid_price
        adjustment = Decimal("1") + (self.costs.slippage_bps / Decimal("10000")) * (1 if intent.side is IntentSide.BUY else -1)
        price = self.rules.executable_price(raw_price * adjustment, intent.side)
        self.rules.validate(quantity=intent.quantity, price=price)
        fee = intent.quantity * price * self.costs.taker_fee_bps / Decimal("10000")
        return Fill(uuid4(), intent.symbol, intent.side, intent.quantity, price, fee, now, intent.correlation_id)
