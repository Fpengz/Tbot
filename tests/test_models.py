from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tbot.core.models import BestBidAsk, IntentSide, OrderIntent


def test_no_trade_intent_has_zero_quantity() -> None:
    now = datetime(2026, 8, 12, tzinfo=UTC)
    intent = OrderIntent.no_trade(
        symbol="BTC-USD", signal_time=now, strategy_id="baseline", rationale="no edge"
    )
    assert intent.side is IntentSide.NO_TRADE
    assert intent.quantity == Decimal(0)
    assert intent.expires_at == now + timedelta(minutes=5)


def test_crossed_book_is_rejected() -> None:
    with pytest.raises(ValueError, match="best bid"):
        BestBidAsk(
            symbol="BTC-USD",
            bid_price=Decimal(100),
            bid_size=Decimal(1),
            ask_price=Decimal(100),
            ask_size=Decimal(1),
            event_time=datetime(2026, 8, 12, tzinfo=UTC),
        )
