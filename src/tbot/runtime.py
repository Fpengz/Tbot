"""Long-running, paper-only supervisor for public data and simulated execution."""
from __future__ import annotations
import asyncio
import json
import signal
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from .data_feed.bars import BarBuilder
from .data_feed.coinbase import stream_messages
from .data_feed.health import FeedHealth
from .data_feed.normalizer import normalize_match, parse_exchange_time
from .data_feed.order_book import Level2Book
from .data_feed.storage import JsonlEventStore
from .monitoring.alerts import AlertManager
from .monitoring.logging import configure_logging, log_event
from .monitoring.server import Health, ObservabilityServer
from .paper import PaperEngine
from .config import load_paper_config
from .execution.simulator import SimulatedVenue, SimulatorCosts
from .portfolio.account import PaperAccount
from .risk.limits import BasicRiskManager, RiskLimits
from .strategy.momentum import MomentumStrategy

@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    symbol: str = "BTC-USD"
    bar_seconds: int = 300
    stale_after_seconds: int = 10
    poll_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    startup_grace_seconds: int = 15

class FeedSequenceGap(RuntimeError):
    """The local book is unsafe until a new snapshot is obtained."""

class CheckpointStore:
    """Atomically replace the latest recoverable paper-account snapshot."""
    def __init__(self, path: Path) -> None: self.path = path
    def save(self, engine: PaperEngine, *, reason: str) -> None:
        payload={"saved_at":datetime.now(UTC).isoformat(),"reason":reason,"cash_usd":str(engine.account.cash_usd),"btc":str(engine.account.btc),"average_entry_price":str(engine.account.average_entry_price),"realized_pnl_usd":str(engine.account.realized_pnl_usd),"fill_count":len(engine.fills)}
        self.path.parent.mkdir(parents=True,exist_ok=True)
        temporary=self.path.with_suffix(self.path.suffix+".tmp")
        temporary.write_text(json.dumps(payload,sort_keys=True)+"\n",encoding="utf-8")
        temporary.replace(self.path)

def _event_time(message: dict[str, Any], received_at: datetime) -> datetime:
    value=message.get("time")
    return parse_exchange_time(str(value)) if value else received_at

class PaperRuntime:
    def __init__(self, *, settings: RuntimeSettings, engine: PaperEngine, data_root: Path, checkpoint: CheckpointStore, alerts: AlertManager) -> None:
        self.settings,self.engine,self.store,self.checkpoint,self.alerts=settings,engine,JsonlEventStore(data_root),checkpoint,alerts
        self.health,self.book,self.bars=FeedHealth(),Level2Book(settings.symbol),BarBuilder(seconds=settings.bar_seconds)
        self.started_at=datetime.now(UTC)
        self.logger=configure_logging()
        self.observability: ObservabilityServer | None = None

    def health_snapshot(self) -> Health:
        now = datetime.now(UTC)
        fresh = self.health.is_fresh(now=now, max_age_seconds=self.settings.stale_after_seconds)
        try:
            self.book.best_bid_ask(); book_ready = True
        except ValueError:
            book_ready = False
        startup = (now - self.started_at).total_seconds() < self.settings.startup_grace_seconds
        detail = {
            "mode": "paper", "symbol": self.settings.symbol, "feed_fresh": fresh,
            "book_ready": book_ready, "startup": startup,
            "last_event_time": self.health.last_event_time,
            "invalid_messages": self.health.invalid_messages,
            "sequence_gaps": self.health.gaps_detected,
        }
        return Health(live=True, ready=(fresh and book_ready) or startup, detail=detail)

    def status_snapshot(self) -> dict[str, Any]:
        return {
            "health": self.health_snapshot().detail,
            "metrics": self.engine.metrics.snapshot(),
            "account": {
                "cash_usd": str(self.engine.account.cash_usd), "btc": str(self.engine.account.btc),
                "realized_pnl_usd": str(self.engine.account.realized_pnl_usd), "fills": len(self.engine.fills),
            },
        }

    def start_observability(self, *, host: str, port: int) -> int:
        self.observability = ObservabilityServer(metrics=self.engine.metrics, health=self.health_snapshot, status=self.status_snapshot)
        return self.observability.start(host, port)
    async def run_session(self, events: AsyncIterator[dict[str, Any]]) -> None:
        iterator=aiter(events)
        # Do not use wait_for(anext(...)): a timeout cancels and closes an async
        # generator, which would tear down an otherwise healthy WebSocket.
        pending=asyncio.create_task(anext(iterator))
        try:
            while True:
                done,_=await asyncio.wait({pending},timeout=self.settings.poll_seconds)
                if not done:
                    self._on_idle()
                    continue
                try:
                    message=pending.result()
                except StopAsyncIteration:
                    return
                pending=asyncio.create_task(anext(iterator))
                self._handle(message,received_at=datetime.now(UTC))
        finally:
            if not pending.done():
                pending.cancel()
                try:
                    await pending
                except asyncio.CancelledError:
                    pass
    def _on_idle(self) -> None:
        now=datetime.now(UTC)
        if self.health.last_event_time is None and (now-self.started_at).total_seconds() < self.settings.startup_grace_seconds:
            return
        if not self.health.is_fresh(now=now,max_age_seconds=self.settings.stale_after_seconds):
            age=float("inf") if self.health.last_event_time is None else (now-self.health.last_event_time).total_seconds()
            self.alerts.stale_data(age); log_event(self.logger,"feed_stale",age_seconds=age)
            self.engine.metrics.increment("feed.stale")
    def _handle(self,message: dict[str,Any],*,received_at: datetime) -> None:
        kind=message.get("type")
        if message.get("product_id") != self.settings.symbol: return
        event_time=_event_time(message,received_at)
        try:
            if kind == "match":
                trade=normalize_match(message,expected_symbol=self.settings.symbol)
                self.store.append(stream=f"trades/symbol={self.settings.symbol}",event=trade,received_at=received_at)
                self.health.record_event(trade.event_time); closed=self.bars.add(trade)
                self.engine.metrics.increment("feed.trade")
                self.engine.metrics.set_gauge("feed.last_event_age_seconds", Decimal("0"))
                if closed is not None: self._on_closed_bar(closed)
            elif kind == "snapshot":
                self.store.append(stream=f"level2/symbol={self.settings.symbol}",event=message,received_at=received_at)
                self.book.apply_snapshot(message,event_time=event_time); self.health.reset_sequence(message.get("sequence")); self.health.record_event(event_time)
                self.engine.metrics.increment("feed.book_snapshot")
            elif kind == "l2update":
                self.store.append(stream=f"level2/symbol={self.settings.symbol}",event=message,received_at=received_at)
                sequence=message.get("sequence")
                if sequence is not None and not self.health.observe_sequence(int(sequence)):
                    self.book=Level2Book(self.settings.symbol)
                    self.alerts.reconciliation_mismatch(f"level2 sequence gap at {sequence}; reconnecting for a new snapshot")
                    raise FeedSequenceGap(f"level2 sequence gap at {sequence}")
                self.book.apply_update(message,event_time=event_time); self.health.record_event(event_time)
                self.engine.metrics.increment("feed.book_update")
            else:
                self.health.invalid_messages += 1
        except FeedSequenceGap:
            self.engine.metrics.increment("feed.sequence_gap")
            raise
        except (KeyError,ValueError,ArithmeticError) as error:
            self.health.invalid_messages += 1; self.engine.metrics.increment("feed.invalid_message"); log_event(self.logger,"invalid_market_event",level=40,error=str(error),message_type=kind)
    def _on_closed_bar(self,bar: Any) -> None:
        try:
            self.engine.on_completed_bar(bar,self.book.best_bid_ask())
            self.checkpoint.save(self.engine,reason="bar_closed")
            log_event(self.logger,"bar_closed",end_time=bar.end_time,fills=len(self.engine.fills))
            self.engine.metrics.increment("bar.closed")
        except (ValueError,ArithmeticError) as error:
            self.alerts.reconciliation_mismatch(f"paper decision skipped: {error}")
            log_event(self.logger,"paper_decision_skipped",error=str(error))
            self.engine.metrics.increment("paper.decision_skipped")
    async def run_forever(self,source: Callable[[],AsyncIterator[dict[str,Any]]],stop: asyncio.Event) -> None:
        delay=1.0
        while not stop.is_set():
            try:
                await self.run_session(source())
                if not stop.is_set(): raise ConnectionError("market-data session ended")
            except asyncio.CancelledError: raise
            except Exception as error:
                self.engine.metrics.increment("feed.session_failed")
                log_event(self.logger,"feed_session_failed",error=str(error),retry_seconds=delay)
                try: await asyncio.wait_for(stop.wait(),timeout=delay)
                except TimeoutError: delay=min(delay*2,self.settings.reconnect_max_seconds)
            finally: self.checkpoint.save(self.engine,reason="session_stop")

def build_runtime(*, config_path: Path, data_root: Path, checkpoint_path: Path) -> PaperRuntime:
    config=load_paper_config(config_path)
    account=PaperAccount(config.starting_cash_usd)
    engine=PaperEngine(MomentumStrategy(quantity=Decimal("0.001")), BasicRiskManager(account,RiskLimits(config.max_position_btc,config.max_order_notional_usd,config.max_daily_loss_usd),kill_switch=config.kill_switch), SimulatedVenue(SimulatorCosts(config.taker_fee_bps,config.slippage_bps)), account)
    logger=configure_logging()
    alerts=AlertManager(lambda alert: log_event(logger,"alert",severity=alert.severity,code=alert.code,message=alert.message))
    return PaperRuntime(settings=RuntimeSettings(symbol=config.symbol,bar_seconds=config.decision_interval_seconds,stale_after_seconds=config.data_stale_after_seconds),engine=engine,data_root=data_root,checkpoint=CheckpointStore(checkpoint_path),alerts=alerts)

async def run_operational(runtime: PaperRuntime, *, duration_seconds: float | None = None, observability_host: str | None = None, observability_port: int = 8080) -> None:
    stop=asyncio.Event(); loop=asyncio.get_running_loop()
    for sig in (signal.SIGINT,signal.SIGTERM):
        try: loop.add_signal_handler(sig,stop.set)
        except NotImplementedError: pass
    if observability_host is not None:
        port = runtime.start_observability(host=observability_host, port=observability_port)
        log_event(runtime.logger, "observability_started", host=observability_host, port=port)
    task=asyncio.create_task(runtime.run_forever(lambda: stream_messages(runtime.settings.symbol),stop))
    try:
        if duration_seconds is None: await stop.wait()
        else: await asyncio.wait_for(stop.wait(),timeout=duration_seconds)
    except TimeoutError: pass
    finally:
        stop.set()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        if runtime.observability is not None:
            runtime.observability.stop()
