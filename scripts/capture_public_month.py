#!/usr/bin/env python3
"""Reliably capture Coinbase public trades and level-2 events for a fixed period."""

from __future__ import annotations

import argparse
import asyncio
import signal
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from tbot.data_feed.coinbase import stream_messages
from tbot.data_feed.normalizer import normalize_match
from tbot.data_feed.storage import JsonlEventStore
from tbot.monitoring.logging import configure_logging, log_event

SYMBOLS = ("BTC-USD", "ETH-USD", "SOL-USD")


async def capture_symbol(
    *, symbol: str, root: Path, deadline: datetime, stop: asyncio.Event, logger: Any
) -> None:
    store = JsonlEventStore(root)
    counts = {"trades": 0, "book_events": 0, "reconnects": 0}
    delay = 1.0

    try:
        while not stop.is_set() and datetime.now(UTC) < deadline:
            remaining = (deadline - datetime.now(UTC)).total_seconds()
            try:
                async with asyncio.timeout(max(remaining, 0.1)):
                    async for message in stream_messages(symbol):
                        if stop.is_set():
                            return
                        received_at = datetime.now(UTC)
                        if message["type"] == "match":
                            event = normalize_match(message, expected_symbol=symbol)
                            store.append(
                                stream=f"trades/symbol={symbol}",
                                event=event,
                                received_at=received_at,
                            )
                            counts["trades"] += 1
                        else:
                            store.append(
                                stream=f"level2/symbol={symbol}",
                                event=message,
                                received_at=received_at,
                            )
                            counts["book_events"] += 1

                if stop.is_set() or datetime.now(UTC) >= deadline:
                    return
                raise ConnectionError("Coinbase feed session ended")
            except TimeoutError:
                return
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - reconnect at the feed boundary
                counts["reconnects"] += 1
                log_event(
                    logger,
                    "capture_session_failed",
                    symbol=symbol,
                    error=str(error),
                    retry_seconds=delay,
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=min(delay, max(remaining, 0.1)))
                except TimeoutError:
                    delay = min(delay * 2, 60.0)
    finally:
        log_event(logger, "capture_finished", symbol=symbol, **counts)


async def main(*, root: Path, days: float, log_file: Path) -> None:
    if days <= 0:
        raise ValueError("days must be positive")

    logger = configure_logging(level="INFO", log_format="json", log_file=log_file)
    started_at = datetime.now(UTC)
    deadline = started_at + timedelta(days=days)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except NotImplementedError:
            pass

    log_event(
        logger,
        "capture_started",
        symbols=list(SYMBOLS),
        root=str(root),
        started_at=started_at,
        deadline=deadline,
        duration_days=days,
    )
    tasks = [
        asyncio.create_task(
            capture_symbol(symbol=symbol, root=root, deadline=deadline, stop=stop, logger=logger),
            name=f"capture-{symbol}",
        )
        for symbol in SYMBOLS
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        stop.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        log_event(logger, "capture_supervisor_stopped", symbols=list(SYMBOLS))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data/raw/live-month"))
    parser.add_argument("--days", type=float, default=30.0)
    parser.add_argument("--log-file", type=Path, default=Path("data/logs/live-month.jsonl"))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(root=args.root, days=args.days, log_file=args.log_file))
