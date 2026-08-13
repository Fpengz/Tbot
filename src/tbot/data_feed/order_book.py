"""Level-2 order-book state with explicit snapshot-before-update semantics."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from tbot.core.models import BestBidAsk


class Level2Book:
    def __init__(self, symbol: str) -> None:
        self.symbol, self.bids, self.asks, self.last_event_time = symbol, {}, {}, None

    def apply_snapshot(self, message: dict[str, Any], *, event_time: datetime) -> None:
        if message.get("product_id") != self.symbol:
            raise ValueError("unexpected product")
        self.bids = {
            Decimal(price): Decimal(size) for price, size in message["bids"] if Decimal(size) > 0
        }
        self.asks = {
            Decimal(price): Decimal(size) for price, size in message["asks"] if Decimal(size) > 0
        }
        self.last_event_time = event_time

    def apply_update(self, message: dict[str, Any], *, event_time: datetime) -> None:
        if self.last_event_time is None:
            raise ValueError("level2 update before snapshot")
        if message.get("product_id") != self.symbol:
            raise ValueError("unexpected product")
        for side, price_text, size_text in message["changes"]:
            levels = self.bids if side == "buy" else self.asks if side == "sell" else None
            if levels is None:
                raise ValueError("invalid level2 side")
            price, size = Decimal(price_text), Decimal(size_text)
            if size < 0:
                raise ValueError("negative level2 size")
            if size == 0:
                levels.pop(price, None)
            else:
                levels[price] = size
        self.last_event_time = event_time

    def best_bid_ask(self) -> BestBidAsk:
        if not self.bids or not self.asks or self.last_event_time is None:
            raise ValueError("book is not tradable")
        bid, ask = max(self.bids), min(self.asks)
        return BestBidAsk(
            self.symbol, bid, self.bids[bid], ask, self.asks[ask], self.last_event_time
        )
