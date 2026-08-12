# tbot

Research-first infrastructure for short-horizon crypto strategies. The initial
scope is BTC-USD spot market data from Coinbase and a local simulated-paper
venue. It has no exchange credentials and cannot submit a real order.

Read [the initial contract](docs/initial-contract.md) before adding a module.

## Local setup

```bash
uv sync --all-extras
uv run pytest
```

`uv` owns `.venv` and [`uv.lock`](uv.lock). Do not install project packages
with bare `pip`; add or update dependencies in `pyproject.toml`, then run
`uv lock` and `uv sync --all-extras`.

## Modes

- `backtest`: historical event replay with simulated fills.
- `paper`: live public market data with simulated fills only.
- `live`: intentionally unsupported. It must not be introduced without an
  explicit operator-approved design and separate user authorization.

## Verification

`uv run pytest` verifies core market-data, strategy, risk, simulation, paper
orchestration, configuration, and monitoring contracts. `uv run tbot
live` must fail; this is an intentional safety check.

## Record public data

After installing `.[live-data]`, record a bounded public-data session with:

```bash
uv run tbot record --seconds 60
```

This writes append-only normalized trade events under `data/raw`. It neither
reads credentials nor contains an exchange order-submission path.

## Download historical research data

Start alpha research with the free Binance Spot archive. This is broad,
high-volume historical data; use your Coinbase recordings for final
venue-matched validation.

```bash
# BTCUSDT 1-minute candles from Jan 2024 through Jul 2026
uv run tbot data binance --kind klines --symbol BTCUSDT --interval 1m \
  --start 2024-01 --end 2026-07

# Aggregate trades are large (hundreds of MB per BTCUSDT month); download one
# month only when you are ready to build microstructure features.
uv run tbot data binance --kind aggTrades --symbol BTCUSDT \
  --start 2026-01 --end 2026-01
```

Files live under `data/historical/` with an adjacent manifest containing each
source URL and SHA-256. Do not mix Binance training data with claimed Coinbase
execution results.

## Run the supervised paper runtime

```bash
uv run tbot runtime
```

It subscribes only to public market data, records raw events, checkpoints paper
state after each closed bar/session, reconnects with backoff, and responds to
`SIGINT`/`SIGTERM` with a clean checkpoint. To run a bounded smoke test, add
`--seconds 60`. Run `uv run tbot --help` for every command and flag.

## Logging and monitoring

The default terminal logs and `tbot status` output use Rich. Every semantic
runtime event can additionally be written as JSON Lines for audit/debugging:

```bash
uv run tbot --log-file data/logs/runtime.jsonl runtime --seconds 60
```

The runtime starts a localhost-only observability server by default. It serves:

- `GET /healthz` — process liveness
- `GET /readyz` — fresh feed and usable book readiness
- `GET /metrics` — Prometheus-compatible plain-text metrics
- `GET /status` — current paper account, health, and metric snapshot

For example, use `uv run tbot runtime --observability-port 8080`; bind to a
non-local address only behind an authenticated network boundary.
