# Current implementation status

Reviewed against the repository on 2026-08-13. This is a descriptive snapshot,
not a claim of production or live-trading readiness.

## Capability matrix

| Area | Implemented now | Important boundary |
| --- | --- | --- |
| Market data | Public Coinbase `matches` and `level2_batch`; normalized trades; raw L2 payload capture; daily append-only JSONL | No authenticated feed, schema versioning, durable quality-event stream, complete duplicate detection, or clock-skew validation |
| Bars and strategy | Deterministic five-minute trade bars; two-bar momentum baseline; explicit `NO_TRADE` | Bars are built directly from trades and omit empty intervals; no feature store or research pipeline |
| Risk | Kill switch, intent expiry, book staleness, notional/position/cash/inventory limits, and a realized-loss threshold | No spread, volatility, order-rate, gross-exposure, drawdown, unknown-state, or reconciliation gate; the loss value is cumulative, not UTC-day scoped |
| Portfolio | Decimal cash/BTC accounting, average entry price, realized P&L, fill idempotency, and atomic checkpoint restoration | No open-order model, unrealized P&L state, or exchange reconciliation |
| Execution | Immediate full simulated fill at the opposite quote plus adverse slippage and taker fee | No latency, book depth, partial fills, cancellation/rejection lifecycle, or exchange-sourced symbol metadata |
| Backtest | Chronological in-memory bar replay, disjoint window helper, and bounded panel jobs with net-cost report | No historical-data loader, training phase, event replay, latency/partial-fill model, or automatic reproducibility manifest |
| Monitoring | Rich/JSON logs, counters/gauges, deduplicated in-process alerts, local HTTP health/status/metrics, and a paper-only FastAPI panel | Alerts only log locally; panel has no authentication, traces, persistence, or external delivery |
| Deployment | Python 3.12 package, locked development environment, tests, Dockerfile, and Compose definition that starts the paper runtime | The image install does not use `uv.lock`, there is no container health check, and paper state remains local to the mounted checkpoint |
| Live trading | Explicitly rejected by the CLI; no authenticated client or order adapter | Live trading is unsupported |

## Runtime path

```text
Coinbase public feed
  -> append trade/L2 events to JSONL
  -> update in-memory book and five-minute trade bar
  -> momentum OrderIntent / NO_TRADE
  -> BasicRiskManager
  -> immediate SimulatedVenue fill
  -> in-memory PaperAccount
  -> write latest restoreable checkpoint
```

The paper and backtest paths reuse the strategy, risk, account, and simulated
venue classes. They do not yet share a replay clock or an event-driven exchange
simulation.

## Review priorities

1. Validate configuration values and relationships, including positive cash,
   fees, slippage, limits, and stale thresholds.
2. Add the missing risk controls from `AGENTS.md`, including spread, volatility,
   order-rate, gross-exposure, drawdown, and unknown-state gates.
3. Add a container health check and install the locked dependency set in the
   container.
4. Build a reproducible historical-data-to-event/bar pipeline and expand the
   simulator before using backtest output for strategy evaluation.
5. Persist every intent,
   risk decision, simulated order, and fill with correlation IDs.

## Verification baseline

At review time, `uv run python -m pytest -q`, `uv run ruff check .`,
`uv run ruff format --check .`, and `uv run ty check` all passed. Tests cover
the core happy paths and selected fail-closed behavior, including checkpoint
restoration, startup readiness, and cash after fees and slippage, but do not
cover end-to-end replay from stored data.
