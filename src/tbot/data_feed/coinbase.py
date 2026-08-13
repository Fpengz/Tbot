"""Public Coinbase WebSocket adapter. It has no authentication or order APIs."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from tbot.core.models import Trade

from .normalizer import normalize_match

COINBASE_PUBLIC_WS = "wss://ws-feed.exchange.coinbase.com"


def subscription(symbol: str) -> dict[str, object]:
    """The single public subscription used for replay-quality capture."""
    # Coinbase Exchange requires authentication for `level2`; `level2_batch`
    # remains public and sends the same snapshot/update schema in 50ms batches.
    return {"type": "subscribe", "product_ids": [symbol], "channels": ["matches", "level2_batch"]}


async def stream_messages(symbol: str = "BTC-USD") -> AsyncIterator[dict[str, Any]]:
    """Yield raw public trade and L2 events from one connection.

    L2 snapshots/updates are deliberately preserved raw. Replay applies them in
    their original order through :class:`Level2Book`.
    """
    try:
        import websockets
    except ImportError as error:  # pragma: no cover
        raise RuntimeError("install the live-data extra to use Coinbase streaming") from error
    # BTC-USD's initial L2 snapshot can exceed the library's 1 MiB default.
    # A bounded application-level recorder plus backpressure is preferable to
    # silently losing the whole session at connection setup.
    async with websockets.connect(COINBASE_PUBLIC_WS, max_size=None, max_queue=1_000) as socket:
        await socket.send(json.dumps(subscription(symbol)))
        async for raw in socket:
            message: dict[str, Any] = json.loads(raw)
            if message.get("product_id") == symbol and message.get("type") in {
                "match",
                "snapshot",
                "l2update",
            }:
                yield message


async def stream_trades(symbol: str = "BTC-USD") -> AsyncIterator[Trade]:
    async for message in stream_messages(symbol):
        if message["type"] == "match":
            yield normalize_match(message, expected_symbol=symbol)


async def record_forever(symbol: str, handler: Any) -> None:
    delay = 1
    while True:  # pragma: no cover
        try:
            async for trade in stream_trades(symbol):
                delay = 1
                await handler(trade)
        except Exception:  # noqa: BLE001 - reconnect at the feed boundary
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)
