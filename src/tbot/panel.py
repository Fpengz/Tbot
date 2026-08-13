"""Local FastAPI control panel for paper runtime, research, and positioning.

The panel deliberately has no live-trading adapter. Runtime controls operate the
public-data/paper supervisor, backtests use the existing chronological replay,
and manual positioning is converted into an intent that must pass the existing
risk manager and simulated venue.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .backtest.replay import run_bars
from .core.models import Bar, BestBidAsk, Fill, IntentSide, OrderIntent
from .data_feed.coinbase import stream_messages
from .execution.simulator import SimulatedVenue, SimulatorCosts
from .monitoring.logging import log_event
from .portfolio.account import PaperAccount
from .risk.limits import BasicRiskManager, RiskLimits
from .runtime import PaperRuntime
from .strategy.momentum import MomentumStrategy

MAX_BACKTEST_BARS = 100_000
MAX_BACKTEST_JOBS = 32
MAX_BACKTEST_WORKERS = 2
MAX_RESULT_FILLS = 10_000


class BarInput(BaseModel):
    """API representation of a completed, UTC-aware decision bar."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=32)
    start_time: datetime
    end_time: datetime
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)
    trade_count: int = Field(ge=0)

    @field_validator("start_time", "end_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("bar timestamps must be timezone-aware")
        return value.astimezone(UTC)

    def to_domain(self) -> Bar:
        return Bar(**self.model_dump())


class BacktestRequest(BaseModel):
    """Bounded momentum replay request; omitted bars use runtime history."""

    model_config = ConfigDict(extra="forbid")

    bars: list[BarInput] | None = Field(default=None, max_length=MAX_BACKTEST_BARS)
    strategy: Literal["momentum-v1"] = "momentum-v1"
    quantity_btc: Decimal = Field(default=Decimal("0.001"), gt=0)
    threshold_bps: Decimal = Field(default=Decimal(10), ge=0)
    starting_cash_usd: Decimal = Field(default=Decimal(10000), gt=0)
    max_position_btc: Decimal = Field(default=Decimal("0.01"), gt=0)
    max_order_notional_usd: Decimal = Field(default=Decimal(250), gt=0)
    max_daily_loss_usd: Decimal = Field(default=Decimal(50), gt=0)
    spread_bps: Decimal = Field(default=Decimal(2), ge=0)
    taker_fee_bps: Decimal = Field(default=Decimal(60), ge=0)
    slippage_bps: Decimal = Field(default=Decimal(5), ge=0)


class TargetPositionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=32)
    target_btc: Decimal = Field(ge=0)


class KillSwitchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool


@dataclass(slots=True)
class BacktestJob:
    job_id: str
    created_at: datetime
    status: Literal["queued", "running", "completed", "failed"] = "queued"
    result: dict[str, Any] | None = None
    error: str | None = None
    task: asyncio.Task[None] | None = field(default=None, repr=False)

    def snapshot(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "created_at": self.created_at.isoformat(),
            "status": self.status,
            "result": self.result,
            "error": self.error,
        }


class PanelService:
    """Coordinates panel actions without replacing strategy/risk contracts."""

    def __init__(
        self,
        runtime: PaperRuntime,
        *,
        max_backtest_jobs: int = MAX_BACKTEST_JOBS,
        max_backtest_workers: int = MAX_BACKTEST_WORKERS,
    ) -> None:
        if max_backtest_jobs <= 0 or max_backtest_workers <= 0:
            raise ValueError("backtest limits must be positive")
        self.runtime = runtime
        self._runtime_task: asyncio.Task[None] | None = None
        self._runtime_stop: asyncio.Event | None = None
        self._position_lock = asyncio.Lock()
        self._backtest_jobs: dict[str, BacktestJob] = {}
        self._backtest_slots = asyncio.Semaphore(max_backtest_workers)

    @property
    def runtime_running(self) -> bool:
        return self._runtime_task is not None and not self._runtime_task.done()

    async def start_runtime(self) -> dict[str, Any]:
        if self.runtime_running:
            return self.runtime_snapshot()
        self._runtime_stop = asyncio.Event()
        self._runtime_task = asyncio.create_task(
            self.runtime.run_forever(
                lambda: stream_messages(self.runtime.settings.symbol), self._runtime_stop
            ),
            name="tbot-paper-runtime",
        )
        log_event(self.runtime.logger, "panel_runtime_started")
        return self.runtime_snapshot()

    async def stop_runtime(self) -> dict[str, Any]:
        task = self._runtime_task
        if task is None:
            return self.runtime_snapshot()
        if self._runtime_stop is not None:
            self._runtime_stop.set()
        if not task.done():
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as error:  # noqa: BLE001 - consume task failure at the control boundary
            log_event(self.runtime.logger, "panel_runtime_failed", level=40, error=str(error))
        self._runtime_task = None
        self._runtime_stop = None
        log_event(self.runtime.logger, "panel_runtime_stopped")
        return self.runtime_snapshot()

    async def close(self) -> None:
        await self.stop_runtime()
        tasks = [job.task for job in self._backtest_jobs.values() if job.task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def set_kill_switch(self, enabled: bool) -> dict[str, Any]:
        risk = self.runtime.engine.risk
        if not isinstance(risk, BasicRiskManager):
            raise TypeError("panel requires the built-in paper risk manager")
        risk.kill_switch = enabled
        log_event(self.runtime.logger, "panel_kill_switch_changed", enabled=enabled)
        return self.runtime_snapshot()

    def runtime_snapshot(self) -> dict[str, Any]:
        risk = self.runtime.engine.risk
        kill_switch = risk.kill_switch if isinstance(risk, BasicRiskManager) else None
        health = self.runtime.health_snapshot()
        return {
            "mode": "paper",
            "symbol": self.runtime.settings.symbol,
            "running": self.runtime_running,
            "kill_switch": kill_switch,
            "health": {**health.detail, "live": health.live, "ready": health.ready},
            "account": self.portfolio_snapshot(),
        }

    def _book_or_none(self) -> BestBidAsk | None:
        try:
            return self.runtime.book.best_bid_ask()
        except ValueError:
            return None

    def portfolio_snapshot(self) -> dict[str, Any]:
        account = self.runtime.engine.account
        book = self._book_or_none()
        last_bar = self.runtime.engine.bars[-1] if self.runtime.engine.bars else None
        mark = book.mid_price if book is not None else last_bar.close if last_bar else None
        unrealized = (
            (mark - account.average_entry_price) * account.btc
            if mark is not None and account.btc > 0
            else Decimal(0)
        )
        equity = account.cash_usd + account.btc * mark if mark is not None else None
        return {
            "symbol": self.runtime.settings.symbol,
            "cash_usd": str(account.cash_usd),
            "position_btc": str(account.btc),
            "average_entry_price_usd": str(account.average_entry_price),
            "realized_pnl_usd": str(account.realized_pnl_usd),
            "unrealized_pnl_usd": str(unrealized),
            "equity_usd": str(equity) if equity is not None else None,
            "mark_price_usd": str(mark) if mark is not None else None,
            "fills": len(self.runtime.engine.fills),
            "open_orders": [],
        }

    def market_snapshot(self, limit: int = 100) -> dict[str, Any]:
        book = self._book_or_none()
        bars = self.runtime.engine.bars[-limit:]
        return {
            "book": self._serialize_book(book) if book is not None else None,
            "bars": [self._serialize_bar(bar) for bar in bars],
        }

    def fills_snapshot(self, limit: int = 100) -> list[dict[str, Any]]:
        return [self._serialize_fill(fill) for fill in self.runtime.engine.fills[-limit:]]

    def strategy_snapshot(self) -> list[dict[str, Any]]:
        return [
            {
                "strategy_id": "momentum-v1",
                "name": "Two-bar momentum",
                "status": "available",
                "parameters": ["quantity_btc", "threshold_bps"],
                "execution": "paper simulator only",
            }
        ]

    def _resolve_bars(self, request: BacktestRequest) -> list[Bar]:
        bars = (
            [bar.to_domain() for bar in request.bars]
            if request.bars is not None
            else list(self.runtime.engine.bars)
        )
        if len(bars) < 2:
            raise ValueError("backtest requires at least two completed bars")
        if len(bars) > MAX_BACKTEST_BARS:
            raise ValueError(f"backtest is limited to {MAX_BACKTEST_BARS} bars")
        symbols = {bar.symbol for bar in bars}
        if symbols != {self.runtime.settings.symbol}:
            raise ValueError(f"backtest supports {self.runtime.settings.symbol} only")
        if bars != sorted(bars, key=lambda bar: bar.end_time):
            raise ValueError("bars must be chronological")
        return bars

    def submit_backtest(self, request: BacktestRequest) -> BacktestJob:
        bars = self._resolve_bars(request)
        self._prune_jobs()
        if len(self._backtest_jobs) >= MAX_BACKTEST_JOBS:
            raise ValueError("too many backtest jobs; wait for existing jobs to finish")
        job = BacktestJob(job_id=str(uuid4()), created_at=datetime.now(UTC))
        self._backtest_jobs[job.job_id] = job
        job.task = asyncio.create_task(self._run_backtest(job, request, bars))
        return job

    async def _run_backtest(
        self, job: BacktestJob, request: BacktestRequest, bars: list[Bar]
    ) -> None:
        job.status = "running"
        try:
            async with self._backtest_slots:
                report = await asyncio.to_thread(self._run_backtest_sync, request, bars)
            job.result = report
            job.status = "completed"
        except asyncio.CancelledError:
            raise
        except (ArithmeticError, KeyError, TypeError, ValueError) as error:
            job.error = str(error)
            job.status = "failed"
        except Exception as error:  # noqa: BLE001 - isolate one research job from the panel
            job.error = f"{type(error).__name__}: {error}"
            job.status = "failed"
            log_event(self.runtime.logger, "panel_backtest_failed", level=40, error=str(error))

    @staticmethod
    def _run_backtest_sync(request: BacktestRequest, bars: list[Bar]) -> dict[str, Any]:
        account = PaperAccount(request.starting_cash_usd)
        venue = SimulatedVenue(SimulatorCosts(request.taker_fee_bps, request.slippage_bps))
        limits = RiskLimits(
            request.max_position_btc,
            request.max_order_notional_usd,
            request.max_daily_loss_usd,
            taker_fee_bps=request.taker_fee_bps,
            slippage_bps=request.slippage_bps,
            price_increment=venue.rules.price_increment,
        )
        result = run_bars(
            bars=bars,
            strategy=MomentumStrategy(
                quantity=request.quantity_btc, threshold_bps=request.threshold_bps
            ),
            risk=BasicRiskManager(account, limits),
            venue=venue,
            account=account,
            spread_bps=request.spread_bps,
        )
        fees = sum((fill.fee for fill in result.fills), Decimal(0))
        net_pnl = result.final_equity_usd - request.starting_cash_usd
        returned_fills = result.fills[-MAX_RESULT_FILLS:]
        return {
            "bars_evaluated": len(bars),
            "fills": [PanelService._serialize_fill(fill) for fill in returned_fills],
            "fills_total": len(result.fills),
            "fills_truncated": len(result.fills) > len(returned_fills),
            "rejected": result.rejected,
            "starting_cash_usd": str(request.starting_cash_usd),
            "final_equity_usd": str(result.final_equity_usd),
            "net_pnl_usd": str(net_pnl),
            "return_pct": str(net_pnl / request.starting_cash_usd * Decimal(100)),
            "total_fees_usd": str(fees),
            "assumptions": {
                "strategy": request.strategy,
                "quantity_btc": str(request.quantity_btc),
                "threshold_bps": str(request.threshold_bps),
                "spread_bps": str(request.spread_bps),
                "taker_fee_bps": str(request.taker_fee_bps),
                "slippage_bps": str(request.slippage_bps),
                "chronological": True,
                "random_split": False,
            },
        }

    def get_backtest(self, job_id: str) -> BacktestJob | None:
        return self._backtest_jobs.get(job_id)

    def list_backtests(self) -> list[dict[str, Any]]:
        jobs = sorted(self._backtest_jobs.values(), key=lambda job: job.created_at, reverse=True)
        return [job.snapshot() for job in jobs]

    def _prune_jobs(self) -> None:
        completed = sorted(
            (
                job
                for job in self._backtest_jobs.values()
                if job.task is not None and job.task.done()
            ),
            key=lambda job: job.created_at,
        )
        for job in completed[: max(0, len(self._backtest_jobs) - MAX_BACKTEST_JOBS + 1)]:
            self._backtest_jobs.pop(job.job_id, None)

    async def target_position(self, request: TargetPositionRequest) -> dict[str, Any]:
        if request.symbol != self.runtime.settings.symbol:
            raise HTTPException(status_code=422, detail="unsupported symbol")
        if not self.runtime_running:
            raise HTTPException(status_code=409, detail="start the paper runtime first")
        health = self.runtime.health_snapshot()
        if not health.ready:
            raise HTTPException(status_code=503, detail="runtime is not ready for positioning")
        async with self._position_lock:
            book = self._book_or_none()
            if book is None:
                raise HTTPException(status_code=503, detail="no usable order book")
            account = self.runtime.engine.account
            delta = request.target_btc - account.btc
            if delta == 0:
                return {"status": "no_change", "portfolio": self.portfolio_snapshot()}
            now = datetime.now(UTC)
            side = IntentSide.BUY if delta > 0 else IntentSide.SELL
            intent = OrderIntent(
                symbol=request.symbol,
                side=side,
                quantity=abs(delta),
                signal_time=now,
                expires_at=now + timedelta(seconds=30),
                strategy_id="panel-positioning-v1",
                rationale="operator paper target-position request",
            )
            decision = self.runtime.engine.risk.assess(intent=intent, book=book, now=now)
            if not decision.approved:
                raise HTTPException(status_code=409, detail=decision.reason)
            try:
                fill = self.runtime.engine.venue.submit(decision=decision, book=book, now=now)
            except (ArithmeticError, ValueError) as error:
                raise HTTPException(
                    status_code=422, detail=f"position violates venue rules: {error}"
                ) from error
            if fill is None:
                raise HTTPException(status_code=409, detail="paper venue did not produce a fill")
            try:
                applied = account.apply_fill(fill)
            except (ArithmeticError, ValueError) as error:
                raise HTTPException(
                    status_code=409, detail=f"paper account rejected fill: {error}"
                ) from error
            if not applied:
                raise HTTPException(status_code=409, detail="paper venue did not produce a fill")
            self.runtime.engine.fills.append(fill)
            self.runtime.engine.metrics.increment("panel.position.fill")
            self.runtime.engine.metrics.set_gauge(
                "account.equity_usd", account.equity(book.mid_price)
            )
            self.runtime.checkpoint.save(self.runtime.engine, reason="panel_position")
            log_event(
                self.runtime.logger,
                "panel_position_filled",
                side=side.value,
                quantity=str(fill.quantity),
                price=str(fill.price),
            )
            return {
                "status": "filled",
                "fill": self._serialize_fill(fill),
                "portfolio": self.portfolio_snapshot(),
            }

    @staticmethod
    def _serialize_book(book: BestBidAsk) -> dict[str, str]:
        return {
            "symbol": book.symbol,
            "bid_price": str(book.bid_price),
            "bid_size": str(book.bid_size),
            "ask_price": str(book.ask_price),
            "ask_size": str(book.ask_size),
            "mid_price": str(book.mid_price),
            "event_time": book.event_time.isoformat(),
        }

    @staticmethod
    def _serialize_bar(bar: Bar) -> dict[str, Any]:
        return {
            "symbol": bar.symbol,
            "start_time": bar.start_time.isoformat(),
            "end_time": bar.end_time.isoformat(),
            "open": str(bar.open),
            "high": str(bar.high),
            "low": str(bar.low),
            "close": str(bar.close),
            "volume": str(bar.volume),
            "trade_count": bar.trade_count,
        }

    @staticmethod
    def _serialize_fill(fill: Fill) -> dict[str, Any]:
        return {
            "order_id": str(fill.order_id),
            "correlation_id": str(fill.correlation_id),
            "symbol": fill.symbol,
            "side": fill.side.value,
            "quantity": str(fill.quantity),
            "price": str(fill.price),
            "fee": str(fill.fee),
            "filled_at": fill.filled_at.isoformat(),
        }


PANEL_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="theme-color" content="#06101b">
  <title>tbot islands — paper runtime in view</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Manrope:wght@400;500;600;700&display=swap');
    :root { color-scheme: dark; font-family: Manrope, sans-serif; background: #06101b; color: #edf8f4; font-synthesis: none; }
    * { box-sizing: border-box; }
    body { margin: 0; min-width: 320px; min-height: 100vh; background: #06101b; }
    button, input { font: inherit; } button { color: inherit; }
    .app { min-height: 100vh; position: relative; overflow: hidden; background: radial-gradient(circle at 50% 64%, rgba(24,105,109,.2), transparent 30%), #06101b; padding-bottom: 44px; }
    .topbar { height: 72px; padding: 0 34px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid rgba(175,220,212,.1); position: relative; z-index: 10; backdrop-filter: blur(18px); background: rgba(5,15,25,.7); }
    .brand { display: flex; align-items: center; gap: 12px; border: 0; background: none; font-size: 16px; font-weight: 700; cursor: pointer; letter-spacing: -.02em; }
    .brand em { font-style: normal; color: #83d8c1; font-weight: 500; }
    .brand-mark { width: 28px; height: 28px; border-radius: 9px; background: #d6faef; display: flex; gap: 2px; align-items: flex-end; justify-content: center; padding: 7px; box-shadow: 0 0 24px rgba(99,230,190,.22); }
    .brand-mark span { display: block; width: 3px; border-radius: 2px; background: #09251f; height: 8px; } .brand-mark span:nth-child(2) { height: 14px; } .brand-mark span:nth-child(3) { height: 10px; }
    .top-actions { display: flex; align-items: center; gap: 9px; }
    .status-pill, .compact-button, .search-trigger, .icon-button { border: 1px solid rgba(178,218,211,.13); background: rgba(255,255,255,.035); border-radius: 10px; height: 36px; display: flex; align-items: center; gap: 8px; padding: 0 12px; cursor: pointer; transition: .2s ease; }
    .status-pill { font-size: 11px; color: #9fb5b2; font-family: 'DM Mono', monospace; }
    .live-dot { display: inline-block; width: 7px; height: 7px; border-radius: 50%; background: #63e6be; box-shadow: 0 0 10px #63e6be; animation: pulse 2.4s infinite; }
    .live-dot.warn { background: #ffb38a; box-shadow: 0 0 10px #ffb38a; }
    .icon-button { width: 36px; padding: 0; justify-content: center; }
    .search-trigger { width: 210px; color: #738b89; justify-content: flex-start; font-size: 12px; } .search-trigger kbd { margin-left: auto; }
    kbd { font: 10px 'DM Mono'; color: #8ba19e; border: 1px solid rgba(255,255,255,.12); border-radius: 5px; padding: 3px 5px; background: rgba(255,255,255,.04); }
    .avatar { border: 0; background: #19342e; color: #bff4e4; width: 36px; height: 36px; border-radius: 50%; font-size: 11px; font-weight: 700; }
    .hero-copy { position: relative; z-index: 2; padding: 54px 4vw 0; width: 50%; }
    .eyebrow { display: flex; align-items: center; gap: 7px; font: 500 10px 'DM Mono'; color: #6bcbb3; letter-spacing: .14em; text-transform: uppercase; }
    h1 { margin: 14px 0 13px; font-size: clamp(40px, 4.4vw, 70px); line-height: 1.01; letter-spacing: -.055em; font-weight: 600; } h1 span { color: #78908e; }
    .hero-copy p { color: #8ba29f; font-size: 14px; margin: 0; max-width: 430px; line-height: 1.7; }
    .summary-row { display: flex; gap: 29px; margin-top: 27px; } .summary-row div { display: flex; align-items: baseline; gap: 7px; } .summary-row strong { font: 500 18px 'DM Mono'; } .summary-row span { font: 10px 'DM Mono'; color: #6c8581; text-transform: uppercase; letter-spacing: .08em; } .summary-row .warm { color: #ffb38a; }
    .ocean { position: absolute; inset: 72px 0 auto; height: 560px; background: radial-gradient(circle at 78% 35%, rgba(99,230,190,.16), transparent 17%), linear-gradient(90deg, #06101b 0%, rgba(6,16,27,.9) 34%, rgba(6,16,27,.08) 70%, rgba(6,16,27,.35) 100%), linear-gradient(180deg, rgba(6,16,27,.55) 0%, transparent 30%, #06101b 97%); opacity: .95; }
    .glow { position: absolute; border-radius: 50%; filter: blur(70px); pointer-events: none; opacity: .16; } .glow-one { width: 320px; height: 180px; background: #19b89b; right: 26%; top: 430px; } .glow-two { width: 180px; height: 120px; background: #ff9b6e; right: 8%; top: 260px; }
    .fleet { position: relative; z-index: 3; margin-top: 164px; padding: 0 4vw; }
    .fleet-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 13px; } .fleet-head h2 { margin: 0; font-size: 12px; font-weight: 600; color: #b4c8c4; }
    .filter-tabs { background: rgba(4,13,22,.68); border: 1px solid rgba(185,224,216,.1); border-radius: 11px; padding: 4px; display: flex; backdrop-filter: blur(12px); }
    .filter-tabs button { border: 0; background: transparent; color: #6f8784; border-radius: 7px; padding: 7px 12px; font: 500 10px 'DM Mono'; cursor: pointer; } .filter-tabs button.active { background: rgba(188,239,226,.1); color: #d8eee9; }
    .compact-button { height: 32px; font: 10px 'DM Mono'; color: #8ba19e; }
    .card-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 13px; }
    .island { text-align: left; border: 1px solid rgba(187,224,217,.11); background: linear-gradient(145deg, rgba(17,34,43,.88), rgba(7,19,29,.91)); backdrop-filter: blur(18px); border-radius: 16px; padding: 15px; min-width: 0; box-shadow: 0 16px 38px rgba(0,0,0,.2); transition: transform .22s ease, border-color .22s ease; }
    .island:hover { transform: translateY(-3px); border-color: rgba(99,230,190,.42); }
    .card-top { display: flex; align-items: center; gap: 9px; } .island-badge { width: 30px; height: 30px; border-radius: 9px; display: grid; place-items: center; background: linear-gradient(145deg, #63e6be, #286d67); color: #071511; font: 700 9px 'DM Mono'; box-shadow: 0 0 20px rgba(99,230,190,.2); } .island-badge.peach { background: linear-gradient(145deg, #ffb38a, #9a4d2e); }
    .island-name { font-size: 12px; font-weight: 700; } .card-status { margin-left: auto; display: flex; align-items: center; gap: 5px; border-radius: 999px; padding: 5px 7px; font: 500 8px 'DM Mono'; text-transform: uppercase; letter-spacing: .05em; background: rgba(99,230,190,.08); color: #73dfc1; } .card-status.waiting { background: rgba(255,179,138,.1); color: #ffb38a; }
    .card-task { margin: 18px 0 13px; display: flex; flex-direction: column; gap: 5px; min-width: 0; } .card-task small { font: 9px 'DM Mono'; text-transform: uppercase; color: #6bcbb3; letter-spacing: .07em; } .card-task strong { font-size: 14px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; } .card-task span { font-size: 10px; color: #708682; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .progress-track { height: 2px; background: rgba(255,255,255,.07); border-radius: 2px; overflow: hidden; } .progress-track span { display: block; height: 100%; width: 64%; background: #63e6be; box-shadow: 0 0 9px #63e6be; animation: progress 4s ease-in-out infinite; } .island.waiting .progress-track span { width: 43%; background: #ffb38a; box-shadow: 0 0 9px #ffb38a; animation: none; }
    .card-meta { display: flex; justify-content: space-between; margin-top: 10px; font: 9px 'DM Mono'; color: #657b78; } .card-meta span { display: flex; align-items: center; gap: 5px; }
    .detail-dock { position: relative; z-index: 3; margin: 16px 4vw 0; border: 1px solid rgba(186,224,216,.1); background: rgba(8,22,31,.86); border-radius: 17px; display: grid; grid-template-columns: 230px 1fr; backdrop-filter: blur(20px); overflow: hidden; box-shadow: 0 20px 50px rgba(0,0,0,.18); }
    .detail-title { padding: 16px; border-right: 1px solid rgba(255,255,255,.07); display: flex; align-items: center; gap: 10px; } .detail-title > div:nth-child(2) { display: flex; flex-direction: column; gap: 4px; min-width: 0; } .detail-title span { font: 8px 'DM Mono'; color: #657c79; letter-spacing: .08em; } .detail-title strong { font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .detail-body { display: grid; grid-template-columns: minmax(250px, 1fr) auto auto; align-items: center; gap: 22px; padding: 13px 15px; } .current-task { display: flex; align-items: flex-start; gap: 11px; } .status-dot { width: 8px; height: 8px; border-radius: 50%; background: #63e6be; box-shadow: 0 0 10px #63e6be; flex: none; margin-top: 6px; } .status-dot.warn { background: #ffb38a; box-shadow: 0 0 10px #ffb38a; } .current-task small { font: 8px 'DM Mono'; color: #75cfb9; text-transform: uppercase; } .current-task h2 { font-size: 13px; margin: 3px 0; } .current-task p { margin: 0; font-size: 9px; color: #708582; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 420px; }
    .stat-strip { display: flex; gap: 20px; } .stat-strip div { display: grid; grid-template-columns: auto auto; column-gap: 6px; align-items: center; } .stat-strip span { font: 8px 'DM Mono'; color: #687d7a; text-transform: uppercase; } .stat-strip strong { font: 9px 'DM Mono'; font-weight: 500; color: #b4c8c4; }
    .jump-button { height: 36px; border: 1px solid rgba(99,230,190,.23); background: #d5f8ed; color: #0b2922; border-radius: 10px; padding: 0 12px; display: flex; align-items: center; gap: 7px; font-size: 10px; font-weight: 700; cursor: pointer; }
    .panel { position: relative; z-index: 3; margin: 16px 4vw 0; display: grid; grid-template-columns: 1fr 1fr; gap: 13px; } .panel section { border: 1px solid rgba(186,224,216,.1); background: rgba(8,22,31,.86); border-radius: 16px; padding: 15px; backdrop-filter: blur(20px); } .panel h2 { margin: 0 0 12px; font-size: 11px; color: #b4c8c4; } .panel h2 span { color: #657c79; font: 8px 'DM Mono'; text-transform: uppercase; letter-spacing: .08em; margin-left: 7px; }
    .metric-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; } .metric { padding: 10px; border: 1px solid rgba(255,255,255,.06); background: rgba(255,255,255,.025); border-radius: 10px; } .metric small { display: block; color: #657b78; font: 8px 'DM Mono'; text-transform: uppercase; letter-spacing: .05em; } .metric strong { display: block; margin-top: 5px; font: 500 12px 'DM Mono'; color: #d8eee9; overflow: hidden; text-overflow: ellipsis; }
    .control-row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; } .control-row input { min-width: 90px; width: 120px; background: rgba(255,255,255,.035); color: #e6f5f1; border: 1px solid rgba(178,218,211,.13); border-radius: 8px; padding: 8px 9px; font: 10px 'DM Mono'; } .control-row button { border: 1px solid rgba(178,218,211,.13); background: rgba(255,255,255,.035); border-radius: 8px; padding: 8px 10px; font: 10px 'DM Mono'; cursor: pointer; } .control-row button.primary { background: #d5f8ed; color: #0b2922; border-color: #d5f8ed; font-weight: 700; } .control-row button.peach { border-color: rgba(255,179,138,.3); color: #ffb38a; }
    pre { white-space: pre-wrap; overflow: auto; max-height: 280px; background: rgba(0,0,0,.18); border: 1px solid rgba(255,255,255,.05); border-radius: 10px; padding: 10px; margin: 0; color: #8fa6a2; font: 9px 'DM Mono'; }
    .notice { color: #ffb38a; font: 9px 'DM Mono'; line-height: 1.6; } footer { position: relative; z-index: 3; margin: 18px 4vw 0; display: flex; justify-content: space-between; color: #536966; font: 8px 'DM Mono'; text-transform: uppercase; letter-spacing: .05em; } footer span { display: flex; align-items: center; gap: 7px; }
    @keyframes pulse { 50% { opacity: .45; transform: scale(.82); } } @keyframes progress { 0%, 100% { width: 52%; } 50% { width: 76%; } }
    @media (max-width: 960px) { .card-grid { grid-template-columns: repeat(2, 1fr); } .hero-copy { width: 68%; } .detail-dock { grid-template-columns: 1fr; } .detail-title { border-right: 0; border-bottom: 1px solid rgba(255,255,255,.07); } .detail-body { grid-template-columns: 1fr auto; } .stat-strip { display: none; } }
    @media (max-width: 650px) { .topbar { padding: 0 18px; } .status-pill, .top-actions > .icon-button { display: none; } .search-trigger { width: 38px; padding: 0; justify-content: center; } .search-trigger span, .search-trigger kbd { display: none; } .hero-copy { padding: 34px 20px 0; width: 92%; } h1 { font-size: 42px; } .fleet { padding: 0 16px; margin-top: 220px; } .fleet-head { overflow-x: auto; } .compact-button { display: none; } .card-grid, .panel { grid-template-columns: 1fr; } .detail-dock { margin: 12px 16px 0; } .detail-body { grid-template-columns: 1fr; padding: 14px; } .jump-button { width: 100%; justify-content: center; } .current-task p { max-width: 76vw; } .panel { margin: 12px 16px 0; } footer { margin: 16px 18px 0; } }
    @media (prefers-reduced-motion: reduce) { * { animation: none !important; } }
  </style>
</head>
<body>
  <main class="app">
    <header class="topbar">
      <button class="brand" onclick="window.scrollTo({top:0,behavior:'smooth'})" aria-label="tbot paper home"><span class="brand-mark"><span></span><span></span><span></span></span><span>tbot <em>islands</em></span></button>
      <nav class="top-actions"><span class="status-pill"><span class="live-dot" id="status-dot"></span><span id="status-label">Paper runtime stopped</span></span><button class="search-trigger" onclick="document.getElementById('target').focus()"><span>◌</span><span>Jump to control…</span><kbd>⌘ K</kbd></button><button class="avatar" title="Paper only">P</button></nav>
    </header>
    <div class="ocean"></div><div class="glow glow-one"></div><div class="glow glow-two"></div>
    <section class="hero-copy"><div class="eyebrow"><span class="live-dot"></span> paper operations / BTC-USD</div><h1>Trade with the<br><span>whole system in view.</span></h1><p>A quiet command surface for runtime health, portfolio state, positioning, and research. Every position request stays behind the paper risk boundary.</p><div class="summary-row"><div><strong id="hero-equity">—</strong><span>equity usd</span></div><div><strong id="hero-position">0</strong><span>btc position</span></div><div><strong class="warm" id="hero-ready">offline</strong><span>readiness</span></div></div></section>
    <section class="fleet"><div class="fleet-head"><div><h2>System islands</h2><div class="eyebrow" style="margin-top:6px">live view / no exchange orders</div></div><div class="filter-tabs"><button class="active">All</button><button onclick="document.getElementById('runtime-panel').scrollIntoView({behavior:'smooth'})">Runtime</button><button onclick="document.getElementById('research-panel').scrollIntoView({behavior:'smooth'})">Research</button></div></div>
      <div class="card-grid">
        <article class="island"><div class="card-top"><div class="island-badge">RT</div><div class="island-name">Runtime</div><div class="card-status" id="runtime-status">Stopped</div></div><div class="card-task"><small>public feed + paper venue</small><strong id="runtime-task">Awaiting operator start</strong><span id="runtime-activity">No market session is active.</span></div><div class="progress-track"><span id="runtime-progress"></span></div><div class="card-meta"><span id="runtime-health">feed offline</span><span>paper only</span></div></article>
        <article class="island"><div class="card-top"><div class="island-badge">PF</div><div class="island-name">Portfolio</div><div class="card-status">In view</div></div><div class="card-task"><small>canonical account state</small><strong id="portfolio-task">Cash-led paper account</strong><span id="portfolio-activity">No open orders; fills are idempotent.</span></div><div class="progress-track"><span style="width:100%;animation:none"></span></div><div class="card-meta"><span id="portfolio-meta">0 fills</span><span>Decimal accounting</span></div></article>
        <article class="island"><div class="card-top"><div class="island-badge peach">PS</div><div class="island-name">Positioning</div><div class="card-status waiting" id="position-status">Guarded</div></div><div class="card-task"><small>risk-approved target</small><strong>Move the paper position</strong><span id="position-activity">Requires fresh feed and usable book.</span></div><div class="progress-track"><span style="width:43%;background:#ffb38a;box-shadow:0 0 9px #ffb38a;animation:none"></span></div><div class="card-meta"><span>risk gate</span><span>simulated fill</span></div></article>
        <article class="island"><div class="card-top"><div class="island-badge">RS</div><div class="island-name">Research</div><div class="card-status">Ready</div></div><div class="card-task"><small>chronological replay</small><strong>Momentum-v1 backtests</strong><span id="research-activity">Net costs included in every report.</span></div><div class="progress-track"><span style="width:100%;animation:none"></span></div><div class="card-meta"><span>bounded jobs</span><span>no random split</span></div></article>
      </div>
    </section>
    <section class="detail-dock"><div class="detail-title"><div class="island-badge" id="dock-badge">RT</div><div><span>selected island</span><strong id="dock-title">Runtime control</strong></div></div><div class="detail-body"><div class="current-task"><span class="status-dot" id="dock-dot"></span><div><small id="dock-eyebrow">paper supervisor</small><h2 id="dock-task">Start the runtime when you are ready.</h2><p id="dock-detail">The panel begins stopped and readiness remains fail-closed until fresh market data and a usable order book arrive.</p></div></div><div class="stat-strip"><div><span>feed</span><strong id="dock-feed">offline</strong></div><div><span>book</span><strong id="dock-book">offline</strong></div><div><span>risk</span><strong id="dock-risk">enabled</strong></div></div><button class="jump-button" onclick="action('/api/runtime/start')">Start runtime ↗</button></div></section>
    <section class="panel" id="runtime-panel"><section><h2>Runtime <span>operator controls</span></h2><div id="runtime-metrics" class="metric-grid"></div><div class="control-row" style="margin-top:12px"><button class="primary" onclick="action('/api/runtime/start')">Start</button><button onclick="action('/api/runtime/stop')">Stop</button><button class="peach" onclick="kill(true)">Kill switch</button><button onclick="kill(false)">Enable risk</button></div></section><section><h2>Portfolio <span>mark + accounting</span></h2><div id="portfolio" class="metric-grid"></div><div class="control-row" style="margin-top:12px"><input id="target" value="0" aria-label="Target BTC" placeholder="target BTC"><button class="primary" onclick="position()">Set paper target</button></div><div class="notice" style="margin-top:10px">Position requests are converted to intents and must pass freshness, cost, venue, cash, inventory, and position limits.</div></section></section>
    <section class="panel" id="research-panel"><section><h2>Research <span>momentum-v1</span></h2><div class="control-row"><input id="qty" value="0.001" aria-label="Backtest quantity" placeholder="quantity BTC"><input id="threshold" value="10" aria-label="Threshold bps" placeholder="threshold bps"><button class="primary" onclick="backtest()">Run replay</button></div><pre id="backtest" style="margin-top:12px">No job submitted.</pre></section><section><h2>Recent fills <span>simulator audit</span></h2><pre id="fills">Loading…</pre></section></section>
    <footer><span><span class="live-dot"></span> Paper simulator only · no credentials · no live venue</span><span>UTC / BTC-USD / tbot 0.1</span></footer>
  </main>
  <script>
    async function get(path) { const response = await fetch(path); const body = await response.json(); if (!response.ok) throw new Error(body.detail || response.statusText); return body; }
    function metrics(obj) { return Object.entries(obj).map(([k,v]) => `<div class="metric"><small>${k.replaceAll('_',' ')}</small><strong>${v ?? '—'}</strong></div>`).join(''); }
    function setText(id, value) { document.getElementById(id).textContent = value ?? '—'; }
    async function refresh() { try { const [runtime, fills] = await Promise.all([get('/api/runtime'), get('/api/portfolio/fills?limit=20')]); const health = runtime.health; const account = runtime.account; const ready = health.ready; setText('status-label', runtime.running ? (ready ? 'All systems ready' : 'Runtime warming up') : 'Paper runtime stopped'); document.getElementById('status-dot').className = `live-dot${ready ? '' : ' warn'}`; setText('hero-equity', account.equity_usd); setText('hero-position', account.position_btc); setText('hero-ready', ready ? 'ready' : runtime.running ? 'warming' : 'offline'); setText('runtime-status', runtime.running ? 'Running' : 'Stopped'); setText('runtime-task', runtime.running ? (ready ? 'Market session is live' : 'Waiting for fresh market data') : 'Awaiting operator start'); setText('runtime-activity', runtime.running ? `${health.feed_fresh ? 'Feed fresh' : 'Feed stale'} · ${health.book_ready ? 'book usable' : 'book unavailable'}` : 'No market session is active.'); setText('runtime-health', ready ? 'ready' : runtime.running ? 'warming' : 'feed offline'); setText('position-activity', ready ? 'Fresh book available for risk review.' : 'Requires fresh feed and usable book.'); setText('portfolio-meta', `${account.fills} fills · ${account.open_orders.length} open orders`); document.getElementById('runtime-metrics').innerHTML = metrics({running: runtime.running, ready, feed_fresh: health.feed_fresh, book_ready: health.book_ready, kill_switch: runtime.kill_switch}); document.getElementById('portfolio').innerHTML = metrics({cash_usd: account.cash_usd, position_btc: account.position_btc, equity_usd: account.equity_usd, realized_pnl: account.realized_pnl_usd, unrealized_pnl: account.unrealized_pnl_usd, mark_usd: account.mark_price_usd}); setText('dock-feed', health.feed_fresh ? 'fresh' : 'stale'); setText('dock-book', health.book_ready ? 'usable' : 'offline'); setText('dock-risk', runtime.kill_switch ? 'killed' : 'enabled'); setText('dock-task', ready ? 'Runtime is ready for paper operations.' : runtime.running ? 'Warm-up is fail-closed.' : 'Start the runtime when you are ready.'); setText('dock-detail', ready ? 'Fresh public data and a usable order book are present. Positioning remains risk-gated.' : 'The panel begins stopped and readiness remains fail-closed until fresh market data and a usable order book arrive.'); document.getElementById('fills').textContent = JSON.stringify(fills, null, 2); } catch (error) { setText('status-label', String(error)); } }
    async function action(path) { try { const response = await fetch(path, {method:'POST'}); if (!response.ok) throw new Error((await response.json()).detail || response.statusText); await refresh(); } catch (error) { alert(error); } }
    async function kill(enabled) { try { await fetch('/api/runtime/kill-switch', {method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify({enabled})}).then(async r => { if (!r.ok) throw new Error((await r.json()).detail); }); await refresh(); } catch (error) { alert(error); } }
    async function position() { try { const response = await fetch('/api/portfolio/position/target', {method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify({symbol:'BTC-USD', target_btc:document.getElementById('target').value})}); const body = await response.json(); if (!response.ok) throw new Error(body.detail); await refresh(); alert(JSON.stringify(body)); } catch (error) { alert(error); } }
    async function backtest() { try { const job = await fetch('/api/backtests', {method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify({quantity_btc:document.getElementById('qty').value, threshold_bps:document.getElementById('threshold').value})}).then(async r => { const b=await r.json(); if (!r.ok) throw new Error(b.detail); return b; }); let result; do { await new Promise(r => setTimeout(r, 250)); result = await get('/api/backtests/' + job.job_id); } while (result.status === 'queued' || result.status === 'running'); document.getElementById('backtest').textContent = JSON.stringify(result, null, 2); } catch (error) { document.getElementById('backtest').textContent = String(error); } }
    refresh(); setInterval(refresh, 2000);
  </script>
</body>
</html>"""


def create_panel_app(runtime: PaperRuntime) -> FastAPI:
    """Create an isolated app instance for one paper runtime."""

    service = PanelService(runtime)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        await service.close()

    app = FastAPI(
        title="tbot paper control panel",
        description="Local paper-only runtime, portfolio, positioning, and backtest panel.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.panel = service

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> str:
        return PANEL_HTML

    @app.get("/api/runtime")
    async def runtime_status() -> dict[str, Any]:
        return service.runtime_snapshot()

    @app.post("/api/runtime/start")
    async def runtime_start() -> dict[str, Any]:
        return await service.start_runtime()

    @app.post("/api/runtime/stop")
    async def runtime_stop() -> dict[str, Any]:
        return await service.stop_runtime()

    @app.post("/api/runtime/kill-switch")
    async def runtime_kill_switch(request: KillSwitchRequest) -> dict[str, Any]:
        return service.set_kill_switch(request.enabled)

    @app.get("/api/strategies")
    async def strategies() -> list[dict[str, Any]]:
        return service.strategy_snapshot()

    @app.get("/api/market")
    async def market(limit: int = Query(default=100, ge=1, le=1000)) -> dict[str, Any]:
        return service.market_snapshot(limit)

    @app.get("/api/portfolio")
    async def portfolio() -> dict[str, Any]:
        return service.portfolio_snapshot()

    @app.get("/api/portfolio/fills")
    async def fills(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
        return service.fills_snapshot(limit)

    @app.post("/api/portfolio/position/target", status_code=200)
    async def target_position(request: TargetPositionRequest) -> dict[str, Any]:
        return await service.target_position(request)

    @app.post("/api/backtests", status_code=202)
    async def submit_backtest(request: BacktestRequest) -> dict[str, Any]:
        try:
            return service.submit_backtest(request).snapshot()
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

    @app.get("/api/backtests")
    async def backtests() -> list[dict[str, Any]]:
        return service.list_backtests()

    @app.get("/api/backtests/{job_id}")
    async def backtest(job_id: str) -> dict[str, Any]:
        job = service.get_backtest(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="backtest job not found")
        return job.snapshot()

    return app
