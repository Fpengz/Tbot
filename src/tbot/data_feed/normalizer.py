"""Normalize Coinbase public feed payloads into exchange-neutral events."""
from __future__ import annotations
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from tbot.core.models import IntentSide, Trade

def parse_exchange_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("exchange time must contain a timezone")
    return parsed.astimezone(UTC)

def normalize_match(message: dict[str, Any], *, expected_symbol: str) -> Trade:
    if message.get("type") != "match":
        raise ValueError("expected Coinbase match message")
    if message.get("product_id") != expected_symbol:
        raise ValueError("unexpected product")
    side_text = message.get("side")
    if side_text not in {"buy", "sell"}:
        raise ValueError("match message has invalid side")
    return Trade(expected_symbol, Decimal(str(message["price"])), Decimal(str(message["size"])),
                 IntentSide(side_text), parse_exchange_time(str(message["time"])), str(message["trade_id"]))
