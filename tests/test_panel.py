import asyncio
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

import tbot.panel as panel_module
from tbot.core.models import Bar
from tbot.execution.simulator import SimulatedVenue, SimulatorCosts
from tbot.monitoring.alerts import AlertManager
from tbot.paper import PaperEngine
from tbot.portfolio.account import PaperAccount
from tbot.risk.limits import BasicRiskManager, RiskLimits
from tbot.runtime import CheckpointStore, PaperRuntime, RuntimeSettings
from tbot.strategy.momentum import MomentumStrategy


def _runtime(tmp_path: Path) -> PaperRuntime:
    account = PaperAccount(Decimal(1000))
    return PaperRuntime(
        settings=RuntimeSettings(),
        engine=PaperEngine(
            MomentumStrategy(quantity=Decimal(".001")),
            BasicRiskManager(
                account,
                RiskLimits(Decimal(".1"), Decimal(250), Decimal(50)),
            ),
            SimulatedVenue(SimulatorCosts(Decimal(0), Decimal(0))),
            account,
        ),
        data_root=tmp_path / "raw",
        checkpoint=CheckpointStore(tmp_path / "state.json"),
        alerts=AlertManager(lambda _: None),
    )


def _bars_payload() -> list[dict[str, object]]:
    start = datetime(2026, 8, 12, tzinfo=UTC)
    return [
        {
            "symbol": "BTC-USD",
            "start_time": (start + timedelta(minutes=index)).isoformat(),
            "end_time": (start + timedelta(minutes=index + 1)).isoformat(),
            "open": str(price),
            "high": str(price),
            "low": str(price),
            "close": str(price),
            "volume": "1",
            "trade_count": 1,
        }
        for index, price in enumerate((1000, 1010, 1020))
    ]


def test_panel_exposes_paper_runtime_portfolio_and_strategy_contract(tmp_path: Path) -> None:
    with TestClient(panel_module.create_panel_app(_runtime(tmp_path))) as client:
        assert "Paper simulator only" in client.get("/").text
        runtime = client.get("/api/runtime")
        assert runtime.status_code == 200
        assert runtime.json()["mode"] == "paper"
        assert runtime.json()["running"] is False
        assert runtime.json()["health"]["ready"] is False
        assert client.get("/api/portfolio").json()["cash_usd"] == "1000"
        assert client.get("/api/strategies").json()[0]["strategy_id"] == "momentum-v1"


def test_panel_backtest_is_bounded_and_reports_net_costed_result(tmp_path: Path) -> None:
    with TestClient(panel_module.create_panel_app(_runtime(tmp_path))) as client:
        response = client.post(
            "/api/backtests",
            json={
                "bars": _bars_payload(),
                "quantity_btc": "0.001",
                "threshold_bps": "1",
                "taker_fee_bps": "10",
                "slippage_bps": "1",
            },
        )
        assert response.status_code == 202
        job_id = response.json()["job_id"]

        deadline = time.monotonic() + 2
        while True:
            result = client.get(f"/api/backtests/{job_id}").json()
            if result["status"] in {"completed", "failed"} or time.monotonic() >= deadline:
                break
            time.sleep(0.01)

        assert result["status"] == "completed"
        assert result["result"]["bars_evaluated"] == 3
        assert result["result"]["fills_total"] == 2
        assert result["result"]["total_fees_usd"] != "0"
        assert result["result"]["assumptions"]["random_split"] is False


def test_panel_positioning_requires_ready_runtime_and_passes_risk(
    tmp_path: Path, monkeypatch
) -> None:
    async def idle_stream(_symbol: str):
        while True:
            await asyncio.sleep(3600)
            yield {}

    monkeypatch.setattr(panel_module, "stream_messages", idle_stream)
    runtime = _runtime(tmp_path)
    with TestClient(panel_module.create_panel_app(runtime)) as client:
        response = client.post(
            "/api/portfolio/position/target",
            json={"symbol": "BTC-USD", "target_btc": "0.01"},
        )
        assert response.status_code == 409

        started = client.post("/api/runtime/start")
        assert started.status_code == 200
        now = datetime.now(UTC)
        runtime._handle(
            {
                "type": "snapshot",
                "product_id": "BTC-USD",
                "bids": [["100", "1"]],
                "asks": [["101", "1"]],
                "time": now.isoformat(),
            },
            received_at=now,
        )
        response = client.post(
            "/api/portfolio/position/target",
            json={"symbol": "BTC-USD", "target_btc": "0.01"},
        )
        assert response.status_code == 200
        assert response.json()["status"] == "filled"
        assert response.json()["portfolio"]["position_btc"] == "0.01"
        assert len(runtime.engine.fills) == 1

        client.post("/api/runtime/kill-switch", json={"enabled": True})
        response = client.post(
            "/api/portfolio/position/target",
            json={"symbol": "BTC-USD", "target_btc": "0.02"},
        )
        assert response.status_code == 409
        assert "kill switch" in response.json()["detail"]


def test_panel_rejects_unknown_backtest_job(tmp_path: Path) -> None:
    with TestClient(panel_module.create_panel_app(_runtime(tmp_path))) as client:
        response = client.get("/api/backtests/not-a-job")
        assert response.status_code == 404


def test_bar_input_normalization_is_utc() -> None:
    payload = _bars_payload()[0]
    payload["start_time"] = "2026-08-12T08:00:00+08:00"
    payload["end_time"] = "2026-08-12T08:01:00+08:00"
    bar = panel_module.BarInput.model_validate(payload).to_domain()
    assert bar.start_time.tzinfo is UTC
    assert isinstance(bar, Bar)
