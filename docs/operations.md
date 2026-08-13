# Operations runbook

Only programmatic backtest helpers and simulated `paper` execution are
supported. No command in this repository can place an exchange order.

## Preflight

1. Synchronize dependencies and run the quality gate:
   `uv run python -m pytest -q`, `uv run ruff check .`, `uv run ruff format
   --check .`, and `uv run ty check`.
2. Verify the host clock is synchronized to UTC and that the target data and log
   volumes have sufficient free space.
3. Review `configs/paper.example.toml`. Keep `mode = "paper"`; set
   `kill_switch = true` when validating ingestion without simulated fills.
4. Run `uv run tbot status --json` and inspect any existing checkpoint. Startup
   restores the account, fill identities, and strategy bar history from it; do
   not delete or replace it unless intentionally starting a new paper account.
5. Confirm `uv run tbot live` exits with status 2.

## Start and observe

Run a bounded session first:

```bash
uv run tbot --log-file data/logs/runtime.jsonl runtime --seconds 60
```

For an ongoing session, omit `--seconds`. The default observability listener is
`127.0.0.1:8080`; do not expose it directly to an untrusted network because it
has no authentication.

The optional local control panel runs the same paper runtime in one process:

```bash
uv run tbot panel
```

It starts with the runtime stopped. Use it for paper start/stop, kill-switch,
portfolio/fill inspection, risk-approved target positions, and bounded
chronological momentum backtests. It has no live-trading endpoint; keep the
panel on localhost or put it behind an authenticated operator boundary.

- `/healthz` reports process liveness.
- `/readyz` reports fresh feed and usable, fresh book readiness. The startup
  grace period is diagnostic warm-up only and never makes the process ready.
- `/metrics` exposes in-process counters and gauges.
- `/status` exposes the current in-memory paper account and health snapshot.

Treat the startup grace as warm-up and verify `feed_fresh` and `book_ready` in
the response. Stop and investigate when the feed is
stale, invalid-message or session-failure counters rise, a sequence gap forces
a reconnect, or any fill is inconsistent with the contemporaneous quote and
configured costs. Sequence checking only runs for payloads that carry a
sequence value; a zero gap count is not proof of complete delivery.

## Shutdown and recovery

Use `SIGINT` or `SIGTERM`. The supervisor writes a final atomic JSON checkpoint
containing balances, realized P&L, average entry price, fill identities, fills,
and strategy bar history. Raw market events are append-only JSONL. A malformed
checkpoint fails startup rather than silently creating a fresh paper account.

Preserve the raw events, JSON audit log, configuration, code revision, and
checkpoint together for analysis. The standalone backtest manifest helper is
not wired into runtime output, so this linkage is currently an operator task.

## Validation boundary

Paper validation is an operational gate, not a unit test. Run across quiet and
volatile periods and manually compare simulated fills with recorded quotes.
Current simulation does not model latency, depth, partial fills, or
cancellations, and the backtest helper synthesizes a spread around bar closes.
Results must not be presented as evidence of profitability or live readiness.

See [current implementation status](current-state.md) for the remaining safety
and research gaps.
