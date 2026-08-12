"""Public-data recorder. It never imports an execution adapter."""
from __future__ import annotations
import asyncio
from datetime import UTC, datetime
from pathlib import Path
from .data_feed.coinbase import stream_messages
from .data_feed.normalizer import normalize_match
from .data_feed.storage import JsonlEventStore
from .monitoring.logging import configure_logging, log_event

async def record(*, symbol: str, root: Path, seconds: float) -> dict[str, int]:
    if seconds <= 0: raise ValueError("seconds must be positive")
    store, logger, counts = JsonlEventStore(root), configure_logging(), {"trades": 0, "book_events": 0}
    try:
        async with asyncio.timeout(seconds):
            async for message in stream_messages(symbol):
                received_at = datetime.now(UTC)
                if message["type"] == "match":
                    store.append(stream=f"trades/symbol={symbol}", event=normalize_match(message, expected_symbol=symbol), received_at=received_at)
                    counts["trades"] += 1
                else:
                    # Preserve raw L2 messages verbatim so snapshot/update replay remains possible.
                    store.append(stream=f"level2/symbol={symbol}", event=message, received_at=received_at)
                    counts["book_events"] += 1
    except TimeoutError:
        pass
    log_event(logger, "recording_finished", symbol=symbol, seconds=seconds, **counts)
    return counts
