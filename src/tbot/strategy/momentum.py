"""A deliberately simple, explainable baseline—not a claim of trading edge."""
from __future__ import annotations
from datetime import datetime, timedelta
from decimal import Decimal
from tbot.core.models import Bar, IntentSide, OrderIntent

class MomentumStrategy:
    strategy_id = "momentum-v1"
    def __init__(self, *, quantity: Decimal, threshold_bps: Decimal = Decimal("10")) -> None:
        self.quantity, self.threshold_bps = quantity, threshold_bps
    def decide_from_bars(self, bars: list[Bar], decision_time: datetime) -> OrderIntent:
        if len(bars) < 2:
            return OrderIntent.no_trade(symbol="BTC-USD", signal_time=decision_time, strategy_id=self.strategy_id, rationale="insufficient history")
        previous, latest = bars[-2], bars[-1]
        if latest.end_time > decision_time:
            raise ValueError("future bar supplied to strategy")
        ret_bps = (latest.close / previous.close - Decimal("1")) * Decimal("10000")
        if abs(ret_bps) < self.threshold_bps:
            return OrderIntent.no_trade(symbol=latest.symbol, signal_time=decision_time, strategy_id=self.strategy_id, rationale="return below threshold")
        side = IntentSide.BUY if ret_bps > 0 else IntentSide.SELL
        return OrderIntent(latest.symbol, side, self.quantity, decision_time, decision_time + timedelta(minutes=5), self.strategy_id, score=ret_bps, rationale="two-bar momentum")
