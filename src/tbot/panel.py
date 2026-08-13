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
  <title>tbot paper control panel</title>
  <style>
    :root { color-scheme: dark; font-family: system-ui, sans-serif; }
    body { max-width: 1180px; margin: 0 auto; padding: 24px; background: #0d1117; color: #e6edf3; }
    h1, h2 { margin: 0 0 12px; } h2 { font-size: 1rem; color: #8b949e; }
    .banner { padding: 12px; margin-bottom: 16px; border: 1px solid #f0883e; color: #ffb77c; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
    section { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
    button, input { background: #21262d; color: #e6edf3; border: 1px solid #484f58; border-radius: 6px; padding: 8px; }
    button { cursor: pointer; } button:hover { border-color: #58a6ff; }
    .row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin: 8px 0; }
    .metric { display: flex; justify-content: space-between; border-bottom: 1px solid #21262d; padding: 6px 0; }
    pre { white-space: pre-wrap; overflow: auto; max-height: 360px; background: #0d1117; padding: 10px; }
    small { color: #8b949e; }
  </style>
</head>
<body>
  <h1>tbot paper control panel</h1>
  <div class="banner">Paper simulator only. No live credentials, live venue, or direct exchange order endpoint exists.</div>
  <div class="grid">
    <section><h2>Runtime</h2><div id="runtime"></div>
      <div class="row"><button onclick="action('/api/runtime/start')">Start</button><button onclick="action('/api/runtime/stop')">Stop</button><button onclick="kill(true)">Kill switch</button><button onclick="kill(false)">Enable risk</button></div>
    </section>
    <section><h2>Portfolio</h2><div id="portfolio"></div>
      <div class="row"><label>Target BTC <input id="target" value="0" size="12"></label><button onclick="position()">Set paper target</button></div>
    </section>
    <section><h2>Backtest momentum-v1</h2>
      <div class="row"><label>Quantity <input id="qty" value="0.001" size="8"></label><label>Threshold bps <input id="threshold" value="10" size="8"></label></div>
      <div class="row"><button onclick="backtest()">Run on runtime bars</button></div><pre id="backtest">No job submitted.</pre>
    </section>
    <section><h2>Recent fills</h2><pre id="fills">Loading…</pre></section>
  </div>
  <script>
    async function get(path) { const response = await fetch(path); const body = await response.json(); if (!response.ok) throw new Error(body.detail || response.statusText); return body; }
    function metrics(obj) { return Object.entries(obj).map(([k,v]) => `<div class="metric"><span>${k}</span><strong>${v ?? '—'}</strong></div>`).join(''); }
    async function refresh() { try { const runtime = await get('/api/runtime'); document.getElementById('runtime').innerHTML = metrics({mode: runtime.mode, running: runtime.running, ready: runtime.health.ready, feed_fresh: runtime.health.feed_fresh, book_ready: runtime.health.book_ready, kill_switch: runtime.kill_switch}); document.getElementById('portfolio').innerHTML = metrics(runtime.account); document.getElementById('fills').textContent = JSON.stringify(await get('/api/portfolio/fills?limit=20'), null, 2); } catch (error) { document.getElementById('runtime').textContent = error; } }
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
