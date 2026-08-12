"""Safe paper-mode orchestration: real/public observations, simulated fills."""
from __future__ import annotations
from dataclasses import dataclass, field
from decimal import Decimal
from tbot.core.models import Bar, BestBidAsk, Fill, IntentSide
from tbot.execution.simulator import SimulatedVenue
from tbot.monitoring.metrics import Metrics
from tbot.portfolio.account import PaperAccount
from tbot.risk.limits import BasicRiskManager
from tbot.strategy.momentum import MomentumStrategy

@dataclass(slots=True)
class PaperEngine:
    strategy: MomentumStrategy
    risk: BasicRiskManager
    venue: SimulatedVenue
    account: PaperAccount
    metrics: Metrics = field(default_factory=Metrics)
    bars: list[Bar] = field(default_factory=list)
    fills: list[Fill] = field(default_factory=list)
    def on_completed_bar(self, bar: Bar, book: BestBidAsk) -> Fill | None:
        """Called only after a complete interval is closed; never trades an open bar."""
        if bar.symbol != book.symbol: raise ValueError("bar/book symbol mismatch")
        if book.event_time > bar.end_time: raise ValueError("book timestamp must not be after decision time")
        self.bars.append(bar)
        intent = self.strategy.decide_from_bars(self.bars, bar.end_time)
        self.metrics.increment(f"intent.{intent.side}")
        decision = self.risk.assess(intent=intent, book=book, now=bar.end_time)
        if not decision.approved:
            self.metrics.increment("risk.rejected")
            return None
        fill = self.venue.submit(decision=decision, book=book, now=bar.end_time)
        if fill:
            self.account.apply_fill(fill); self.fills.append(fill); self.metrics.increment("fill.count")
            self.metrics.set_gauge("account.equity_usd", self.account.equity(book.mid_price))
        return fill
