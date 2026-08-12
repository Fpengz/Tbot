"""Non-secret runtime configuration with paper/backtest-only validation."""
from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
import tomllib

@dataclass(frozen=True, slots=True)
class PaperConfig:
    mode: str
    symbol: str
    decision_interval_seconds: int
    starting_cash_usd: Decimal
    max_position_btc: Decimal
    max_order_notional_usd: Decimal
    max_daily_loss_usd: Decimal
    kill_switch: bool
    data_stale_after_seconds: int
    taker_fee_bps: Decimal
    slippage_bps: Decimal

def load_paper_config(path: Path) -> PaperConfig:
    with path.open("rb") as handle: raw = tomllib.load(handle)
    runtime, simulator, risk = raw["runtime"], raw["simulator"], raw["risk"]
    if runtime["mode"] not in {"paper", "backtest"}: raise ValueError("only paper and backtest config modes are supported")
    if runtime["symbol"] != "BTC-USD": raise ValueError("initial contract supports BTC-USD only")
    if runtime["decision_interval_seconds"] != 300: raise ValueError("initial contract uses 5-minute decisions")
    return PaperConfig(runtime["mode"], runtime["symbol"], runtime["decision_interval_seconds"], Decimal(simulator["starting_cash_usd"]), Decimal(risk["max_position_btc"]), Decimal(risk["max_order_notional_usd"]), Decimal(risk["max_daily_loss_usd"]), risk["kill_switch"], runtime["data_stale_after_seconds"], Decimal(simulator["taker_fee_bps"]), Decimal(simulator["slippage_bps"]))
