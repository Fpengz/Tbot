"""Single Typer command surface for the paper-only trading system."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from .config import load_paper_config
from .historical.binance import SUPPORTED_KINDS, download_range
from .monitoring.logging import configure_logging
from .record import record as record_public_data
from .runtime import build_runtime, run_operational

app = typer.Typer(
    name="tbot",
    help="Research-first BTC-USD paper-trading system. Live trading is unavailable.",
    no_args_is_help=True,
    add_completion=False,
)
data_app = typer.Typer(
    help="Acquire reproducible historical research datasets.", no_args_is_help=True
)
app.add_typer(data_app, name="data")

ConfigPath = Annotated[
    Path,
    typer.Option("--config", exists=True, readable=True, help="Paper-only TOML configuration."),
]
DataPath = Annotated[Path, typer.Option("--data-dir", help="Append-only raw-data directory.")]


@app.callback()
def root(
    log_level: Annotated[
        str, typer.Option("--log-level", help="DEBUG, INFO, WARNING, ERROR.")
    ] = "INFO",
    log_format: Annotated[
        str, typer.Option("--log-format", help="Operator console format: rich or json.")
    ] = "rich",
    log_file: Annotated[
        Path | None, typer.Option("--log-file", help="Optional JSONL audit log path.")
    ] = None,
) -> None:
    """Configure consistent operator and machine-readable observability."""
    if log_format not in {"rich", "json"}:
        raise typer.BadParameter("log format must be rich or json")
    configure_logging(level=log_level, log_format=log_format, log_file=log_file)


@app.command("record")
def record_command(
    symbol: Annotated[str, typer.Option("--symbol", help="Public Coinbase product.")] = "BTC-USD",
    seconds: Annotated[
        float, typer.Option("--seconds", min=0.1, help="Bounded recording duration.")
    ] = 60,
    data_dir: DataPath = Path("data/raw"),
) -> None:
    """Record public trades and level-2 batches; no order code is involved."""
    counts = asyncio.run(record_public_data(symbol=symbol, root=data_dir, seconds=seconds))
    typer.echo(json.dumps(counts, sort_keys=True))


@data_app.command("binance")
def download_binance_command(
    kind: Annotated[
        str,
        typer.Option(
            "--kind", case_sensitive=False, help="Archive kind: klines, aggTrades, or trades."
        ),
    ] = "klines",
    symbol: Annotated[str, typer.Option("--symbol", help="Binance Spot symbol.")] = "BTCUSDT",
    start: Annotated[str, typer.Option("--start", help="First month, YYYY-MM.")] = "2024-01",
    end: Annotated[str, typer.Option("--end", help="Last month, YYYY-MM.")] = "2026-07",
    interval: Annotated[
        str, typer.Option("--interval", help="Kline interval; required for --kind klines.")
    ] = "1m",
    data_dir: DataPath = Path("data/historical"),
) -> None:
    """Download Binance public Spot archives and write a provenance manifest."""
    normalized_kind = kind.lower()
    if normalized_kind not in SUPPORTED_KINDS:
        raise typer.BadParameter(f"kind must be one of: {', '.join(sorted(SUPPORTED_KINDS))}")
    files = download_range(
        root=data_dir,
        kind=normalized_kind,
        symbol=symbol,
        start=start,
        end=end,
        interval=interval if normalized_kind == "klines" else None,
    )
    typer.echo(
        json.dumps(
            {
                "downloaded": len(files),
                "kind": normalized_kind,
                "symbol": symbol.upper(),
                "data_dir": str(data_dir),
            },
            sort_keys=True,
        )
    )


@app.command("runtime")
def runtime_command(
    config: ConfigPath = Path("configs/paper.example.toml"),
    data_dir: DataPath = Path("data/raw"),
    checkpoint: Annotated[
        Path, typer.Option("--checkpoint", help="Atomic paper-account checkpoint.")
    ] = Path("data/state/paper-account.json"),
    seconds: Annotated[
        float | None,
        typer.Option("--seconds", min=0.1, help="Optional bounded smoke-test duration."),
    ] = None,
    observability_host: Annotated[
        str | None,
        typer.Option(
            "--observability-host", help="Bind status/health/metrics server; e.g. 127.0.0.1."
        ),
    ] = "127.0.0.1",
    observability_port: Annotated[
        int,
        typer.Option(
            "--observability-port",
            min=0,
            max=65535,
            help="Status server port; use 0 for a free port.",
        ),
    ] = 8080,
) -> None:
    """Run the resilient public-data and simulated-paper supervisor."""
    runtime = build_runtime(config_path=config, data_root=data_dir, checkpoint_path=checkpoint)
    asyncio.run(
        run_operational(
            runtime,
            duration_seconds=seconds,
            observability_host=observability_host,
            observability_port=observability_port,
        )
    )


@app.command("panel")
def panel_command(
    config: ConfigPath = Path("configs/paper.example.toml"),
    data_dir: DataPath = Path("data/raw"),
    checkpoint: Annotated[
        Path, typer.Option("--checkpoint", help="Atomic paper-account checkpoint.")
    ] = Path("data/state/paper-account.json"),
    host: Annotated[
        str, typer.Option("--host", help="Panel bind address; local-only by default.")
    ] = "127.0.0.1",
    port: Annotated[
        int, typer.Option("--port", min=0, max=65535, help="Panel port; use 0 for a free port.")
    ] = 8000,
) -> None:
    """Run the local paper-only FastAPI control panel."""
    import uvicorn

    from .panel import create_panel_app

    runtime = build_runtime(config_path=config, data_root=data_dir, checkpoint_path=checkpoint)
    uvicorn.run(create_panel_app(runtime), host=host, port=port, log_level="info")


@app.command("status")
def status_command(
    config: ConfigPath = Path("configs/paper.example.toml"),
    checkpoint: Annotated[
        Path, typer.Option("--checkpoint", help="Optional checkpoint to inspect.")
    ] = Path("data/state/paper-account.json"),
    as_json: Annotated[
        bool, typer.Option("--json", help="Emit JSON rather than a Rich table.")
    ] = False,
) -> None:
    """Show safe runtime configuration and the latest checkpoint, if present."""
    settings = load_paper_config(config)
    payload: dict[str, object] = {
        "mode": settings.mode,
        "symbol": settings.symbol,
        "execution_venue": "simulated",
        "live_trading": False,
        "kill_switch": settings.kill_switch,
    }
    if checkpoint.exists():
        payload["checkpoint"] = json.loads(checkpoint.read_text(encoding="utf-8"))
    if as_json:
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    table = Table(title="tbot status", show_header=True, header_style="bold cyan")
    table.add_column("Field")
    table.add_column("Value")
    for key, value in payload.items():
        table.add_row(
            key, json.dumps(value, sort_keys=True) if isinstance(value, dict) else str(value)
        )
    Console().print(table)


@app.command("live")
def live_command() -> None:
    """Always reject live trading: no live adapter exists in this project."""
    typer.echo("Live mode is unsupported; no order-submission adapter exists.", err=True)
    raise typer.Exit(code=2)


@app.command("version")
def version_command() -> None:
    """Print the CLI version."""
    typer.echo("tbot 0.1.0")


def main() -> None:
    app()
