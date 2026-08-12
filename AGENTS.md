# AGENTS.md

## Project purpose

Build a research-first, safety-first crypto trading system for short-horizon
(initially 5–10 minute) strategies. The first supported market is one liquid
spot pair on one exchange (for example, BTC-USD). The system must be usable in
research, backtest, paper, and live modes without changing strategy logic.

Do not treat a directional prediction as a trade signal by itself. Every
decision must be evaluated after fees, spread, slippage, latency, and risk
limits. `NO_TRADE` is a valid and expected decision.

## Technology choices

- Use Python 3.12+ for the initial system.
- Prefer typed Python, `asyncio`, `polars` or `pandas`, Parquet, and DuckDB.
- Use UTC everywhere: event timestamps, storage partitions, logs, and APIs.
- Keep dependencies small and choose maintained, official exchange APIs when
  possible.
- Do not introduce C++ or Rust unless a measured production constraint justifies
  it; strategy research remains language-independent.

## Module boundaries

Keep these components separate. Depend on interfaces/contracts, not concrete
exchange clients or databases.

```text
Exchange -> Data feed -> Strategy -> Risk manager -> Execution -> Exchange
                                 |                    |
                         Portfolio/account state <--- fills/orders

Backtesting replaces the live feed and execution venue with historical replay
and a simulated exchange. Monitoring observes every component. Infrastructure
supports every environment.
```

### Data feed

- Ingest trades, quotes/order-book updates, and derived bars from an exchange.
- Persist immutable raw events before or alongside derived data when practical.
- Validate sequence gaps, schema changes, duplicate events, clock skew, and
  stale streams; expose health metrics.
- Never silently fabricate missing data. Mark gaps explicitly.

### Strategy

- Consume only data available at the decision timestamp.
- Emit an `OrderIntent` / target position, never an exchange order.
- Include signal timestamp, symbol, side, confidence or score, rationale, and
  expiry. Make feature calculations deterministic and replayable.
- Make `LONG`, `SHORT`, `FLAT`, and `NO_TRADE` explicit. Spot-only deployments
  must reject unsupported short intents.

### Risk manager

- Is the only component permitted to approve an intent for execution.
- Enforce per-order and per-symbol limits, gross exposure, available balance,
  maximum daily loss/drawdown, order-rate limits, spread/volatility filters,
  stale-data protection, and an operator kill switch.
- Fail closed: on unknown account state, stale data, feed/exchange failure, or
  violated limits, reject new orders. Cancelling open orders and flattening a
  position require an explicit, audited policy.

### Portfolio/account state

- Maintain one canonical view of cash, positions, open orders, fills, fees,
  realized P&L, and unrealized P&L.
- Reconcile regularly with the exchange and raise an alert on discrepancies.
- Treat exchange fills and account updates as authoritative; make event handling
  idempotent.

### Execution

- Convert only risk-approved intents into exchange-specific orders.
- Track acknowledgements, partial fills, cancellations, rejections, retries,
  fees, and order lifecycle transitions.
- Use idempotency/client order IDs where the exchange supports them. Never
  blind-retry an order submission without first resolving its state.
- Log the expected price, actual fill price, fees, and latency for every order.

### Backtesting and simulation

- Share strategy and risk contracts with live trading.
- Use chronological, walk-forward evaluation only; never randomly shuffle time
  series or leak future data into features.
- Model bid/ask spread, fees, realistic slippage, latency, partial fills, and
  exchange constraints. Report assumptions with every result.
- Keep raw datasets, feature versions, parameters, code revision, and results
  linked so a backtest is reproducible.

### Monitoring and visualization

- Record structured logs, metrics, traces/errors, decisions, risk rejections,
  orders, fills, P&L, exposure, and feed freshness.
- Provide dashboards for current position, P&L, open orders, signal history,
  risk state, and system health.
- Alert on process failure, stale data, reconciliation mismatch, rejected or
  unexpected orders, limit breaches, and connectivity problems.

### Infrastructure and deployment

- Keep separate configurations, credentials, and endpoints for development,
  backtest, paper, and live trading. Default to paper mode.
- Load secrets from environment variables or a secret manager; never commit API
  keys, private keys, `.env` files, or account identifiers.
- Use containers/process supervision, health checks, pinned dependencies,
  automated tests, and restart-safe storage.
- Any live deployment needs an explicit configuration flag and a manual operator
  confirmation outside normal test workflows.

## Suggested repository layout

```text
src/
  data_feed/       # exchange adapters, normalization, storage
  strategy/        # features, signals, strategy implementations
  risk/            # limits and intent approval
  portfolio/       # canonical account and position state
  execution/       # order lifecycle and exchange adapters
  backtest/        # replay clock, simulator, reports
  monitoring/      # metrics, dashboards, alerts
  core/            # shared domain models and interfaces
tests/
configs/           # non-secret config templates only
scripts/           # operational entry points
docs/
```

## Engineering rules

- Make units explicit: price, base quantity, quote quantity, and percentages
  must not be interchangeable.
- Use `Decimal` or integer exchange units for order prices, sizes, balances, and
  P&L; do not use binary floating point for monetary accounting.
- Validate exchange symbol metadata, tick size, step size, minimum notional,
  and supported order types before submitting orders.
- Prefer append-only event logs and immutable market-data files. Never overwrite
  raw data or trade records.
- Use structured logging with UTC timestamps and correlation IDs; redact secrets.
- Make all order and fill handling idempotent and restart-safe.
- Add unit tests for strategy/risk rules and integration tests with mocked
  exchange responses. Add regression tests for bugs involving orders or P&L.
- Before enabling live trading, require successful backtests, sustained paper
  trading, reconciliation checks, monitoring/alerts, and tested kill-switch
  behavior.

## Safety constraints

- Never enable live trading, create exchange credentials, place orders, or move
  funds unless the user explicitly requests that exact action.
- Never weaken risk checks merely to make a backtest or demo trade more often.
- Do not claim profitability from accuracy alone. Report net performance after
  all modeled costs, drawdown, and out-of-sample validation.
- If uncertain about an exchange API response or order state, stop new order
  submission, reconcile, and surface the uncertainty.
