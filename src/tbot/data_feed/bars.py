"""Deterministic UTC bar aggregation from normalized trades."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta
from decimal import Decimal

from tbot.core.models import Bar, Trade


class BarBuilder:
    """Incrementally closes complete UTC trade bars without inventing empty bars."""

    def __init__(self, *, seconds: int = 300) -> None:
        if seconds <= 0:
            raise ValueError("seconds must be positive")
        self.seconds = seconds
        self._trades: list[Trade] = []
        self._start: datetime | None = None

    def add(self, trade: Trade) -> Bar | None:
        epoch = int(trade.event_time.timestamp())
        start = trade.event_time.fromtimestamp(
            epoch - epoch % self.seconds, tz=trade.event_time.tzinfo
        )
        if self._start is None:
            self._start = start
        if start < self._start:
            raise ValueError("out-of-order trade cannot be added to incremental bars")
        if start == self._start:
            self._trades.append(trade)
            return None
        closed = aggregate_bars(self._trades, seconds=self.seconds)
        if len(closed) != 1:
            raise RuntimeError("bar builder invariant violated")
        self._start, self._trades = start, [trade]
        return closed[0]


def aggregate_bars(trades: Iterable[Trade], *, seconds: int = 60) -> list[Bar]:
    if seconds <= 0:
        raise ValueError("seconds must be positive")
    buckets: dict[tuple[str, datetime], list[Trade]] = defaultdict(list)
    for trade in sorted(trades, key=lambda item: item.event_time):
        epoch = int(trade.event_time.timestamp())
        start = trade.event_time.fromtimestamp(epoch - epoch % seconds, tz=trade.event_time.tzinfo)
        buckets[(trade.symbol, start)].append(trade)
    result: list[Bar] = []
    for (symbol, start), items in sorted(buckets.items(), key=lambda pair: pair[0][1]):
        prices = [item.price for item in items]
        result.append(
            Bar(
                symbol,
                start,
                start + timedelta(seconds=seconds),
                prices[0],
                max(prices),
                min(prices),
                prices[-1],
                sum((item.size for item in items), Decimal(0)),
                len(items),
            )
        )
    return result
