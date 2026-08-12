from datetime import UTC, datetime, timedelta
from decimal import Decimal
import pytest
from tbot.data_feed.health import FeedHealth
from tbot.data_feed.order_book import Level2Book

def test_level2_snapshot_then_update() -> None:
    now=datetime(2026,8,12,tzinfo=UTC); book=Level2Book("BTC-USD")
    book.apply_snapshot({"product_id":"BTC-USD","bids":[["100","1"]],"asks":[["101","2"]]},event_time=now)
    book.apply_update({"product_id":"BTC-USD","changes":[["buy","100","0"],["buy","99","3"]]},event_time=now)
    assert book.best_bid_ask().bid_price == Decimal("99")

def test_update_without_snapshot_rejected() -> None:
    with pytest.raises(ValueError,match="before snapshot"):
        Level2Book("BTC-USD").apply_update({"product_id":"BTC-USD","changes":[]},event_time=datetime(2026,8,12,tzinfo=UTC))

def test_feed_freshness() -> None:
    now=datetime(2026,8,12,tzinfo=UTC); health=FeedHealth(); health.record_event(now)
    assert health.is_fresh(now=now+timedelta(seconds=9),max_age_seconds=10)
    assert not health.is_fresh(now=now+timedelta(seconds=11),max_age_seconds=10)

def test_feed_sequence_marks_gaps_and_duplicates() -> None:
    health=FeedHealth(); assert health.observe_sequence(10); assert not health.observe_sequence(12)
    assert not health.observe_sequence(12) and health.gaps_detected == 1 and health.duplicate_events == 1
