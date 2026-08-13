from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tbot.core.models import IntentSide, Trade
from tbot.data_feed.bars import aggregate_bars
from tbot.data_feed.coinbase import subscription
from tbot.data_feed.normalizer import normalize_match


def test_normalizes_coinbase_match() -> None:
    trade = normalize_match(
        {
            "type": "match",
            "product_id": "BTC-USD",
            "price": "100.1",
            "size": "0.2",
            "side": "buy",
            "time": "2026-08-12T00:00:03Z",
            "trade_id": 7,
        },
        expected_symbol="BTC-USD",
    )
    assert trade.price == Decimal("100.1")
    assert trade.side is IntentSide.BUY


def test_aggregate_trades_into_one_minute_bar() -> None:
    trades = [
        Trade(
            "BTC-USD",
            Decimal(100),
            Decimal(1),
            IntentSide.BUY,
            datetime(2026, 8, 12, 0, 0, 1, tzinfo=UTC),
            "1",
        ),
        Trade(
            "BTC-USD",
            Decimal(102),
            Decimal(2),
            IntentSide.SELL,
            datetime(2026, 8, 12, 0, 0, 59, tzinfo=UTC),
            "2",
        ),
    ]
    bar = aggregate_bars(trades)[0]
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (
        Decimal(100),
        Decimal(102),
        Decimal(100),
        Decimal(102),
        Decimal(3),
    )


def test_normalizer_rejects_other_symbol() -> None:
    with pytest.raises(ValueError, match="unexpected product"):
        normalize_match({"type": "match", "product_id": "ETH-USD"}, expected_symbol="BTC-USD")


def test_subscription_captures_trade_and_level2_channels() -> None:
    assert subscription("BTC-USD")["channels"] == ["matches", "level2_batch"]
