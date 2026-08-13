# Initial system contract

This document defines the milestone contract. It is normative where it says
"must". The repository is not yet complete against the contract; see
[current implementation status](current-state.md) for the implemented subset
and known gaps.

## Scope

| Item | Decision |
| --- | --- |
| Market | `BTC-USD` spot only |
| Market-data venue | Coinbase Exchange public WebSocket feed |
| Execution venue | Local simulator only |
| Trading modes | Backtest and paper |
| Decision interval | Every 5 minutes, aligned to UTC boundaries |
| Holding horizon | 5–15 minutes; strategy-specific and explicit |
| Order intent | `BUY`, `SELL`, or `NO_TRADE` |
| Initial execution model | Immediate simulated taker fill |
| Price basis | USD per BTC; all amounts use `Decimal` |
| Time basis | UTC, timezone-aware timestamps |

## Required market inputs

- Public trades.
- Public `level2_batch` order-book snapshots and updates. The recorder preserves
  their raw order, and the live paper runtime applies them through `Level2Book`.
  Stored-event replay into the book is not implemented yet.
- Five-minute trade bars derived directly from normalized trades. A future
  research pipeline may persist one-minute bars and derive decision bars from
  them, but that pipeline is not implemented.

The feed appends normalized trades and raw level-2 payloads to daily JSONL
partitions. The runtime counts invalid messages and conditionally checks a
sequence when an incoming payload contains one. It never fabricates empty bars.
Durable data-quality events, complete duplicate detection, clock-skew checks,
and guaranteed sequence-gap detection remain contract requirements rather than
implemented capabilities.

## Decision contract

A decision is triggered when the first trade in a later interval closes the
previous five-minute bar. The paper engine rejects a book timestamp later than
the bar end, and risk rejects a stale book. The strategy emits a time-limited
`OrderIntent` or `NO_TRADE`; it cannot call an exchange client. Risk is the sole
approval gate and the simulator accepts only approved decisions.

## Paper simulator assumptions

The simulator charges a configured taker fee, starts from the opposite best
quote, applies configurable adverse slippage, rounds price adversely, and
checks hard-coded price/quantity increments and minimum notional. Approved
orders fill immediately and completely. It does not model latency, depth,
partial fills, cancellations, order lifecycle, or venue-fetched symbol rules.
Experiment manifests exist as a standalone helper but are not automatically
created by replay or paper runs.

## Safety invariants

1. No live credentials, private keys, or order-submission endpoint are part of
   this milestone.
2. Stale books reject new intents. Unknown account-state representation and
   exchange reconciliation are still required before any live-capable design.
3. Intents and fills carry correlation IDs and UTC timestamps. Complete durable
   decision/rejection/order transition logging is still required.
4. Monetary models use `Decimal`. Current simulator symbol rules are local
   defaults; exchange-metadata validation is still required.
