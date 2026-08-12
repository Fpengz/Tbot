from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from tbot.core.models import Bar, BestBidAsk, Fill
from tbot.execution.simulator import SimulatedVenue
from tbot.portfolio.account import PaperAccount
from tbot.risk.limits import BasicRiskManager
from tbot.strategy.momentum import MomentumStrategy

@dataclass(frozen=True, slots=True)
class BacktestResult:
    fills: tuple[Fill, ...]
    final_equity_usd: Decimal
    rejected: int

@dataclass(frozen=True, slots=True)
class WalkForwardResult:
    folds: tuple[BacktestResult, ...]
    def total_final_equity(self) -> Decimal:
        return sum((fold.final_equity_usd for fold in self.folds), Decimal("0"))

def run_bars(*, bars: list[Bar], strategy: MomentumStrategy, risk: BasicRiskManager, venue: SimulatedVenue, account: PaperAccount, spread_bps: Decimal = Decimal("2")) -> BacktestResult:
    if bars != sorted(bars, key=lambda b: b.end_time): raise ValueError("bars must be chronological")
    fills: list[Fill] = []; rejected = 0
    for index in range(1, len(bars)):
        current = bars[index]
        half = spread_bps / Decimal("20000")
        book = BestBidAsk(current.symbol, current.close * (1-half), Decimal("1"), current.close * (1+half), Decimal("1"), current.end_time)
        intent = strategy.decide_from_bars(bars[:index+1], current.end_time)
        decision = risk.assess(intent=intent, book=book, now=current.end_time)
        fill = venue.submit(decision=decision, book=book, now=current.end_time)
        if fill: account.apply_fill(fill); fills.append(fill)
        elif intent.side.value != "no_trade": rejected += 1
    mark = bars[-1].close if bars else Decimal("0")
    return BacktestResult(tuple(fills), account.equity(mark), rejected)

def walk_forward(*, bars: list[Bar], test_bars: int, build_strategy: callable, build_risk: callable, build_venue: callable, starting_cash_usd: Decimal) -> WalkForwardResult:
    """Evaluate disjoint chronological test windows; no random time-series split."""
    if test_bars < 2: raise ValueError("test_bars must be at least two")
    folds: list[BacktestResult] = []
    for start in range(0, len(bars), test_bars):
        window = bars[start:start+test_bars]
        if len(window) < 2: break
        account = PaperAccount(starting_cash_usd)
        folds.append(run_bars(bars=window, strategy=build_strategy(), risk=build_risk(account), venue=build_venue(), account=account))
    return WalkForwardResult(tuple(folds))
