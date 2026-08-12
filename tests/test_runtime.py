import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from tbot.execution.simulator import SimulatedVenue, SimulatorCosts
from tbot.monitoring.alerts import AlertManager
from tbot.paper import PaperEngine
from tbot.portfolio.account import PaperAccount
from tbot.risk.limits import BasicRiskManager, RiskLimits
from tbot.runtime import CheckpointStore, FeedSequenceGap, PaperRuntime, RuntimeSettings, build_runtime
from tbot.strategy.momentum import MomentumStrategy

def _engine() -> PaperEngine:
    account=PaperAccount(Decimal("1000"))
    return PaperEngine(MomentumStrategy(quantity=Decimal(".01"), threshold_bps=Decimal("1")), BasicRiskManager(account,RiskLimits(Decimal("1"),Decimal("100"),Decimal("20"))), SimulatedVenue(SimulatorCosts(Decimal("0"),Decimal("0"))),account)

async def _messages(start: datetime):
    yield {"type":"snapshot","product_id":"BTC-USD","bids":[["100","1"]],"asks":[["101","1"]],"time":start.isoformat()}
    for minute, price in ((1,"100"),(5,"101"),(6,"101"),(10,"102"),(11,"102")):
        moment=start+timedelta(minutes=minute)
        if minute in (5,10): yield {"type":"l2update","product_id":"BTC-USD","changes":[["buy","100","1"],["sell","101","1"]],"time":moment.isoformat()}
        yield {"type":"match","product_id":"BTC-USD","price":price,"size":".1","side":"buy","time":moment.isoformat(),"trade_id":str(minute)}

def test_runtime_records_closes_bars_and_checkpoints(tmp_path: Path) -> None:
    alerts=[]; engine=_engine(); runtime=PaperRuntime(settings=RuntimeSettings(poll_seconds=1),engine=engine,data_root=tmp_path/"raw",checkpoint=CheckpointStore(tmp_path/"state.json"),alerts=AlertManager(alerts.append))
    asyncio.run(runtime.run_session(_messages(datetime(2026,8,12,tzinfo=UTC))))
    assert (tmp_path/"state.json").exists()
    assert len(engine.bars) == 2 and len(engine.fills) == 1
    assert list((tmp_path/"raw"/"trades"/"symbol=BTC-USD").rglob("events.jsonl"))

def test_runtime_builds_from_paper_only_config(tmp_path: Path) -> None:
    runtime=build_runtime(config_path=Path("configs/paper.example.toml"),data_root=tmp_path/"raw",checkpoint_path=tmp_path/"state.json")
    assert runtime.settings.symbol == "BTC-USD" and runtime.engine.risk.kill_switch is False

def test_runtime_does_not_cancel_live_source_on_health_poll(tmp_path: Path) -> None:
    runtime=PaperRuntime(settings=RuntimeSettings(poll_seconds=.001),engine=_engine(),data_root=tmp_path/"raw",checkpoint=CheckpointStore(tmp_path/"state.json"),alerts=AlertManager(lambda _: None))
    async def delayed_snapshot():
        await asyncio.sleep(.01)
        yield {"type":"snapshot","product_id":"BTC-USD","bids":[["100","1"]],"asks":[["101","1"]],"time":"2026-08-12T00:00:00Z"}
    asyncio.run(runtime.run_session(delayed_snapshot()))
    assert runtime.book.best_bid_ask().bid_price == Decimal("100")

def test_sequence_gap_invalidates_book_and_propagates_for_reconnect(tmp_path: Path) -> None:
    runtime=PaperRuntime(settings=RuntimeSettings(),engine=_engine(),data_root=tmp_path/"raw",checkpoint=CheckpointStore(tmp_path/"state.json"),alerts=AlertManager(lambda _: None))
    now=datetime(2026,8,12,tzinfo=UTC)
    runtime._handle({"type":"snapshot","product_id":"BTC-USD","bids":[["100","1"]],"asks":[["101","1"]],"sequence":1,"time":now.isoformat()},received_at=now)
    import pytest
    with pytest.raises(FeedSequenceGap): runtime._handle({"type":"l2update","product_id":"BTC-USD","changes":[],"sequence":3,"time":now.isoformat()},received_at=now)
