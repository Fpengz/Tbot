# Initial system contract

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
| Initial order type | Simulated marketable limit order |
| Price basis | USD per BTC; all amounts use `Decimal` |
| Time basis | UTC, timezone-aware timestamps |

## Required market inputs

- Public trades.
- Public `level2_batch` order-book snapshots and updates. The recorder preserves their raw
  order, then replay applies snapshot/update events through `Level2Book`.
- One-minute bars derived from recorded events; five-minute decision bars derived
  from the one-minute bars.

The feed writes normalized, append-only raw events. A missing sequence, stale
feed, duplicate event, or invalid payload is recorded as a data-quality event;
it must not be silently filled.

## Decision contract

At each 5-minute UTC boundary, a strategy sees only events whose exchange
timestamp is at or before that boundary. It emits a time-limited `OrderIntent`
or `NO_TRADE`; it cannot call an exchange client. Risk is the sole approval
gate. The execution layer accepts only approved intents.

## Paper simulator assumptions

The simulator starts conservative: it charges configured maker/taker fees,
crosses the current best bid/ask for marketable orders, applies configurable
slippage, respects symbol increments and minimum notional, and records partial
or rejected fills. Every assumption is versioned with the result.

## Safety invariants

1. No live credentials, private keys, or order-submission endpoint are part of
   this milestone.
2. Unknown or stale account/feed state rejects new intents.
3. Every decision, rejection, order, fill, and state transition receives a UTC
   timestamp and correlation ID.
4. Exchange metadata determines price/size rounding; never use binary floats
   for monetary values.
