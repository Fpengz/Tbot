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
from uuid import UUID

from .config import load_paper_config
from .core.models import Bar, Fill, IntentSide
from .data_feed.bars import BarBuilder
from .data_feed.coinbase import stream_messages
from .data_feed.health import FeedHealth
from .data_feed.normalizer import normalize_match, parse_exchange_time
from .data_feed.order_book import Level2Book
from .data_feed.storage import JsonlEventStore
from .execution.simulator import SimulatedVenue, SimulatorCosts
from .monitoring.alerts import AlertManager
from .monitoring.logging import configure_logging, log_event
from .monitoring.server import Health, ObservabilityServer
from .paper import PaperEngine
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

    def __init__(self, path: Path) -> None:
        self.path = path

    def save(self, engine: PaperEngine, *, reason: str) -> None:
        account = {
            "cash_usd": str(engine.account.cash_usd),
            "btc": str(engine.account.btc),
            "average_entry_price": str(engine.account.average_entry_price),
            "realized_pnl_usd": str(engine.account.realized_pnl_usd),
            "seen_fill_ids": sorted(str(fill_id) for fill_id in engine.account.seen_fill_ids),
        }
        fills = [
            {
                "order_id": str(fill.order_id),
                "symbol": fill.symbol,
                "side": fill.side.value,
                "quantity": str(fill.quantity),
                "price": str(fill.price),
                "fee": str(fill.fee),
                "filled_at": fill.filled_at.astimezone(UTC).isoformat(),
                "correlation_id": str(fill.correlation_id),
            }
            for fill in engine.fills
        ]
        bars = [
            {
                "symbol": bar.symbol,
                "start_time": bar.start_time.astimezone(UTC).isoformat(),
                "end_time": bar.end_time.astimezone(UTC).isoformat(),
                "open": str(bar.open),
                "high": str(bar.high),
                "low": str(bar.low),
                "close": str(bar.close),
                "volume": str(bar.volume),
                "trade_count": bar.trade_count,
            }
            for bar in engine.bars
        ]
        payload = {
            "schema_version": 1,
            "saved_at": datetime.now(UTC).isoformat(),
            "reason": reason,
            # Keep the summary fields for status consumers while the nested
            # state is the authoritative restore payload.
            **account,
            "fill_count": len(engine.fills),
            "account": account,
            "fills": fills,
            "bars": bars,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(self.path)

    def restore(self, engine: PaperEngine) -> bool:
        """Restore account, fill idempotency, and strategy history if present."""
        if not self.path.exists():
            return False
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise TypeError("checkpoint root must be an object")
            if payload.get("schema_version") != 1:
                raise ValueError("unsupported checkpoint schema")
            account = payload["account"]
            fills = [self._decode_fill(item) for item in payload["fills"]]
            bars = [self._decode_bar(item) for item in payload["bars"]]
            seen_fill_ids = {UUID(value) for value in account["seen_fill_ids"]}
            fill_ids = {fill.order_id for fill in fills}
            if len(fill_ids) != len(fills) or seen_fill_ids != fill_ids:
                raise ValueError("checkpoint fill identities do not match fill records")
            engine.account.restore_state(
                cash_usd=Decimal(account["cash_usd"]),
                btc=Decimal(account["btc"]),
                realized_pnl_usd=Decimal(account["realized_pnl_usd"]),
                average_entry_price=Decimal(account["average_entry_price"]),
                seen_fill_ids=seen_fill_ids,
            )
            engine.fills.extend(fills)
            engine.bars.extend(bars)
        except (
            AttributeError,
            IndexError,
            KeyError,
            TypeError,
            ValueError,
            ArithmeticError,
            UnicodeError,
            json.JSONDecodeError,
        ) as error:
            raise ValueError(f"invalid paper-account checkpoint: {self.path}") from error
        return True

    @staticmethod
    def _decode_datetime(value: str) -> datetime:
        if not isinstance(value, str):
            raise TypeError("checkpoint timestamp must be text")
        result = datetime.fromisoformat(value)
        if result.tzinfo is None:
            raise ValueError("checkpoint timestamp must be timezone-aware")
        return result

    @classmethod
    def _decode_fill(cls, item: dict[str, Any]) -> Fill:
        return Fill(
            UUID(item["order_id"]),
            item["symbol"],
            IntentSide(item["side"]),
            Decimal(item["quantity"]),
            Decimal(item["price"]),
            Decimal(item["fee"]),
            cls._decode_datetime(item["filled_at"]),
            UUID(item["correlation_id"]),
        )

    @classmethod
    def _decode_bar(cls, item: dict[str, Any]) -> Bar:
        return Bar(
            item["symbol"],
            cls._decode_datetime(item["start_time"]),
            cls._decode_datetime(item["end_time"]),
            Decimal(item["open"]),
            Decimal(item["high"]),
            Decimal(item["low"]),
            Decimal(item["close"]),
            Decimal(item["volume"]),
            int(item["trade_count"]),
        )


def _event_time(message: dict[str, Any], received_at: datetime) -> datetime:
    value = message.get("time")
    return parse_exchange_time(str(value)) if value else received_at


class PaperRuntime:
    def __init__(
        self,
        *,
        settings: RuntimeSettings,
        engine: PaperEngine,
        data_root: Path,
        checkpoint: CheckpointStore,
        alerts: AlertManager,
    ) -> None:
        self.settings, self.engine, self.store, self.checkpoint, self.alerts = (
            settings,
            engine,
            JsonlEventStore(data_root),
            checkpoint,
            alerts,
        )
        self.health, self.book, self.bars = (
            FeedHealth(),
            Level2Book(settings.symbol),
            BarBuilder(seconds=settings.bar_seconds),
        )
        self.started_at = datetime.now(UTC)
        self.logger = configure_logging()
        self.observability: ObservabilityServer | None = None

    def health_snapshot(self) -> Health:
        now = datetime.now(UTC)
        fresh = self.health.is_fresh(now=now, max_age_seconds=self.settings.stale_after_seconds)
        try:
            book = self.book.best_bid_ask()
            book_age = (now - book.event_time).total_seconds()
            book_ready = 0 <= book_age <= self.settings.stale_after_seconds
        except ValueError:
            book_ready = False
        startup = (now - self.started_at).total_seconds() < self.settings.startup_grace_seconds
        detail = {
            "mode": "paper",
            "symbol": self.settings.symbol,
            "feed_fresh": fresh,
            "book_ready": book_ready,
            "startup": startup,
            "last_event_time": self.health.last_event_time,
            "invalid_messages": self.health.invalid_messages,
            "sequence_gaps": self.health.gaps_detected,
        }
        return Health(live=True, ready=fresh and book_ready, detail=detail)

    def status_snapshot(self) -> dict[str, Any]:
        return {
            "health": self.health_snapshot().detail,
            "metrics": self.engine.metrics.snapshot(),
            "account": {
                "cash_usd": str(self.engine.account.cash_usd),
                "btc": str(self.engine.account.btc),
                "realized_pnl_usd": str(self.engine.account.realized_pnl_usd),
                "fills": len(self.engine.fills),
            },
        }

    def start_observability(self, *, host: str, port: int) -> int:
        self.observability = ObservabilityServer(
            metrics=self.engine.metrics, health=self.health_snapshot, status=self.status_snapshot
        )
        return self.observability.start(host, port)

    async def run_session(self, events: AsyncIterator[dict[str, Any]]) -> None:
        iterator = aiter(events)

        async def next_message() -> dict[str, Any]:
            return await anext(iterator)

        # Do not use wait_for(anext(...)): a timeout cancels and closes an async
        # generator, which would tear down an otherwise healthy WebSocket.
        pending = asyncio.create_task(next_message())
        try:
            while True:
                done, _ = await asyncio.wait({pending}, timeout=self.settings.poll_seconds)
                if not done:
                    self._on_idle()
                    continue
                try:
                    message = pending.result()
                except StopAsyncIteration:
                    return
                pending = asyncio.create_task(next_message())
                self._handle(message, received_at=datetime.now(UTC))
        finally:
            if not pending.done():
                pending.cancel()
                try:
                    await pending
                except asyncio.CancelledError:
                    pass

    def _on_idle(self) -> None:
        now = datetime.now(UTC)
        if (
            self.health.last_event_time is None
            and (now - self.started_at).total_seconds() < self.settings.startup_grace_seconds
        ):
            return
        if not self.health.is_fresh(now=now, max_age_seconds=self.settings.stale_after_seconds):
            age = (
                float("inf")
                if self.health.last_event_time is None
                else (now - self.health.last_event_time).total_seconds()
            )
            self.alerts.stale_data(age)
            log_event(self.logger, "feed_stale", age_seconds=age)
            self.engine.metrics.increment("feed.stale")

    def _handle(self, message: dict[str, Any], *, received_at: datetime) -> None:
        kind = message.get("type")
        if message.get("product_id") != self.settings.symbol:
            return
        event_time = _event_time(message, received_at)
        try:
            if kind == "match":
                trade = normalize_match(message, expected_symbol=self.settings.symbol)
                self.store.append(
                    stream=f"trades/symbol={self.settings.symbol}",
                    event=trade,
                    received_at=received_at,
                )
                self.health.record_event(trade.event_time)
                closed = self.bars.add(trade)
                self.engine.metrics.increment("feed.trade")
                self.engine.metrics.set_gauge("feed.last_event_age_seconds", Decimal(0))
                if closed is not None:
                    self._on_closed_bar(closed)
            elif kind == "snapshot":
                self.store.append(
                    stream=f"level2/symbol={self.settings.symbol}",
                    event=message,
                    received_at=received_at,
                )
                self.book.apply_snapshot(message, event_time=event_time)
                self.health.reset_sequence(message.get("sequence"))
                self.health.record_event(event_time)
                self.engine.metrics.increment("feed.book_snapshot")
            elif kind == "l2update":
                self.store.append(
                    stream=f"level2/symbol={self.settings.symbol}",
                    event=message,
                    received_at=received_at,
                )
                sequence = message.get("sequence")
                if sequence is not None and not self.health.observe_sequence(int(sequence)):
                    self.book = Level2Book(self.settings.symbol)
                    self.alerts.reconciliation_mismatch(
                        f"level2 sequence gap at {sequence}; reconnecting for a new snapshot"
                    )
                    raise FeedSequenceGap(f"level2 sequence gap at {sequence}")
                self.book.apply_update(message, event_time=event_time)
                self.health.record_event(event_time)
                self.engine.metrics.increment("feed.book_update")
            else:
                self.health.invalid_messages += 1
        except FeedSequenceGap:
            self.engine.metrics.increment("feed.sequence_gap")
            raise
        except (KeyError, ValueError, ArithmeticError) as error:
            self.health.invalid_messages += 1
            self.engine.metrics.increment("feed.invalid_message")
            log_event(
                self.logger, "invalid_market_event", level=40, error=str(error), message_type=kind
            )

    def _on_closed_bar(self, bar: Any) -> None:
        try:
            self.engine.on_completed_bar(bar, self.book.best_bid_ask())
            self.checkpoint.save(self.engine, reason="bar_closed")
            log_event(
                self.logger, "bar_closed", end_time=bar.end_time, fills=len(self.engine.fills)
            )
            self.engine.metrics.increment("bar.closed")
        except (ValueError, ArithmeticError) as error:
            self.alerts.reconciliation_mismatch(f"paper decision skipped: {error}")
            log_event(self.logger, "paper_decision_skipped", error=str(error))
            self.engine.metrics.increment("paper.decision_skipped")

    async def run_forever(
        self, source: Callable[[], AsyncIterator[dict[str, Any]]], stop: asyncio.Event
    ) -> None:
        delay = 1.0
        while not stop.is_set():
            try:
                await self.run_session(source())
                if not stop.is_set():
                    raise ConnectionError("market-data session ended")
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - reconnect at the feed boundary
                self.engine.metrics.increment("feed.session_failed")
                log_event(self.logger, "feed_session_failed", error=str(error), retry_seconds=delay)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    delay = min(delay * 2, self.settings.reconnect_max_seconds)
            finally:
                self.checkpoint.save(self.engine, reason="session_stop")


def build_runtime(*, config_path: Path, data_root: Path, checkpoint_path: Path) -> PaperRuntime:
    config = load_paper_config(config_path)
    account = PaperAccount(config.starting_cash_usd)
    checkpoint = CheckpointStore(checkpoint_path)
    venue = SimulatedVenue(SimulatorCosts(config.taker_fee_bps, config.slippage_bps))
    limits = RiskLimits(
        config.max_position_btc,
        config.max_order_notional_usd,
        config.max_daily_loss_usd,
        data_stale_after_seconds=config.data_stale_after_seconds,
        taker_fee_bps=config.taker_fee_bps,
        slippage_bps=config.slippage_bps,
        price_increment=venue.rules.price_increment,
    )
    engine = PaperEngine(
        MomentumStrategy(quantity=Decimal("0.001")),
        BasicRiskManager(account, limits, kill_switch=config.kill_switch),
        venue,
        account,
    )
    checkpoint.restore(engine)
    logger = configure_logging()
    alerts = AlertManager(
        lambda alert: log_event(
            logger, "alert", severity=alert.severity, code=alert.code, message=alert.message
        )
    )
    return PaperRuntime(
        settings=RuntimeSettings(
            symbol=config.symbol,
            bar_seconds=config.decision_interval_seconds,
            stale_after_seconds=config.data_stale_after_seconds,
        ),
        engine=engine,
        data_root=data_root,
        checkpoint=checkpoint,
        alerts=alerts,
    )


async def run_operational(
    runtime: PaperRuntime,
    *,
    duration_seconds: float | None = None,
    observability_host: str | None = None,
    observability_port: int = 8080,
) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    if observability_host is not None:
        port = runtime.start_observability(host=observability_host, port=observability_port)
        log_event(runtime.logger, "observability_started", host=observability_host, port=port)
    task = asyncio.create_task(
        runtime.run_forever(lambda: stream_messages(runtime.settings.symbol), stop)
    )
    try:
        if duration_seconds is None:
            await stop.wait()
        else:
            await asyncio.wait_for(stop.wait(), timeout=duration_seconds)
    except TimeoutError:
        pass
    finally:
        stop.set()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        if runtime.observability is not None:
            runtime.observability.stop()
