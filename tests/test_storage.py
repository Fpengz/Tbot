from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from tbot.core.models import IntentSide, Trade
from tbot.data_feed.storage import JsonlEventStore


def test_store_writes_daily_append_only_partition(tmp_path: Path) -> None:
    time = datetime(2026, 8, 12, tzinfo=UTC)
    event = Trade("BTC-USD", Decimal(1), Decimal(1), IntentSide.BUY, time, "x")
    path = JsonlEventStore(tmp_path).append(
        stream="trades/symbol=BTC-USD", event=event, received_at=time
    )
    assert path == tmp_path / "trades/symbol=BTC-USD/date=2026-08-12/events.jsonl"
    assert '"symbol": "BTC-USD"' in path.read_text()
