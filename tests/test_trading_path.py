from datetime import UTC, datetime, timedelta
from decimal import Decimal
from tbot.backtest.replay import run_bars, walk_forward
from tbot.core.models import Bar, BestBidAsk, IntentSide, OrderIntent
from tbot.execution.simulator import SimulatedVenue, SimulatorCosts, SymbolRules
from tbot.portfolio.account import PaperAccount
from tbot.risk.limits import BasicRiskManager, RiskLimits
from tbot.strategy.momentum import MomentumStrategy
from tbot.paper import PaperEngine

def _book(now: datetime) -> BestBidAsk:
    return BestBidAsk("BTC-USD", Decimal("100"), Decimal("1"), Decimal("101"), Decimal("1"), now)

def test_risk_rejects_stale_data() -> None:
    now = datetime(2026, 8, 12, tzinfo=UTC)
    intent = OrderIntent("BTC-USD", IntentSide.BUY, Decimal(".01"), now, now+timedelta(minutes=5), "test")
    risk = BasicRiskManager(PaperAccount(Decimal("1000")), RiskLimits(Decimal("1"), Decimal("100"), Decimal("20")))
    assert not risk.assess(intent=intent, book=_book(now-timedelta(seconds=11)), now=now).approved

def test_simulated_fill_is_applied_once() -> None:
    now = datetime(2026, 8, 12, tzinfo=UTC); account = PaperAccount(Decimal("1000"))
    intent = OrderIntent("BTC-USD", IntentSide.BUY, Decimal(".1"), now, now+timedelta(minutes=5), "test")
    decision = BasicRiskManager(account, RiskLimits(Decimal("1"), Decimal("100"), Decimal("20"))).assess(intent=intent, book=_book(now), now=now)
    fill = SimulatedVenue(SimulatorCosts(Decimal("10"), Decimal("0"))).submit(decision=decision, book=_book(now), now=now)
    assert fill and account.apply_fill(fill) and not account.apply_fill(fill)

def test_spot_risk_rejects_uncovered_sell() -> None:
    now = datetime(2026, 8, 12, tzinfo=UTC)
    intent = OrderIntent("BTC-USD", IntentSide.SELL, Decimal(".01"), now, now+timedelta(minutes=5), "test")
    risk = BasicRiskManager(PaperAccount(Decimal("1000")), RiskLimits(Decimal("1"), Decimal("100"), Decimal("20")))
    assert "insufficient BTC" in risk.assess(intent=intent, book=_book(now), now=now).reason

def test_replay_runs_chronologically() -> None:
    start=datetime(2026,8,12,tzinfo=UTC)
    bars=[Bar("BTC-USD", start+timedelta(minutes=i), start+timedelta(minutes=i+1), Decimal(str(100+i)), Decimal(str(100+i)), Decimal(str(100+i)), Decimal(str(100+i)), Decimal("1"), 1) for i in range(3)]
    account=PaperAccount(Decimal("1000")); result=run_bars(bars=bars, strategy=MomentumStrategy(quantity=Decimal(".01"),threshold_bps=Decimal("1")), risk=BasicRiskManager(account,RiskLimits(Decimal("1"),Decimal("100"),Decimal("20"))), venue=SimulatedVenue(SimulatorCosts(Decimal("0"),Decimal("0"))),account=account)
    assert len(result.fills) == 2

def test_paper_engine_uses_simulated_venue_only() -> None:
    now=datetime(2026,8,12,tzinfo=UTC); account=PaperAccount(Decimal("1000"))
    engine=PaperEngine(MomentumStrategy(quantity=Decimal(".01"),threshold_bps=Decimal("1")), BasicRiskManager(account,RiskLimits(Decimal("1"),Decimal("100"),Decimal("20"))), SimulatedVenue(SimulatorCosts(Decimal("0"),Decimal("0"))),account)
    first=Bar("BTC-USD",now-timedelta(minutes=2),now-timedelta(minutes=1),Decimal("100"),Decimal("100"),Decimal("100"),Decimal("100"),Decimal("1"),1)
    second=Bar("BTC-USD",now-timedelta(minutes=1),now,Decimal("101"),Decimal("101"),Decimal("101"),Decimal("101"),Decimal("1"),1)
    engine.on_completed_bar(first,_book(now-timedelta(minutes=1)))
    assert engine.on_completed_bar(second,_book(now)) is not None

def test_simulator_enforces_exchange_increment() -> None:
    now=datetime(2026,8,12,tzinfo=UTC); intent=OrderIntent("BTC-USD",IntentSide.BUY,Decimal(".015"),now,now+timedelta(minutes=5),"test")
    decision=BasicRiskManager(PaperAccount(Decimal("1000")),RiskLimits(Decimal("1"),Decimal("100"),Decimal("20"))).assess(intent=intent,book=_book(now),now=now)
    import pytest
    with pytest.raises(ValueError,match="quantity violates"):
        SimulatedVenue(SimulatorCosts(Decimal("0"),Decimal("0")),SymbolRules(quantity_increment=Decimal(".01"))).submit(decision=decision,book=_book(now),now=now)

def test_walk_forward_uses_disjoint_chronological_windows() -> None:
    start=datetime(2026,8,12,tzinfo=UTC); bars=[Bar("BTC-USD",start+timedelta(minutes=i),start+timedelta(minutes=i+1),Decimal("100"),Decimal("100"),Decimal("100"),Decimal("100"),Decimal("1"),1) for i in range(4)]
    result=walk_forward(bars=bars,test_bars=2,starting_cash_usd=Decimal("100"),build_strategy=lambda: MomentumStrategy(quantity=Decimal(".01")),build_risk=lambda a: BasicRiskManager(a,RiskLimits(Decimal("1"),Decimal("100"),Decimal("20"))),build_venue=lambda: SimulatedVenue(SimulatorCosts(Decimal("0"),Decimal("0"))))
    assert len(result.folds) == 2
