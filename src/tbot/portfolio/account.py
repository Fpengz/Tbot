from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from uuid import UUID

from tbot.core.models import Fill, IntentSide


@dataclass(slots=True)
class PaperAccount:
    cash_usd: Decimal
    btc: Decimal = Decimal(0)
    realized_pnl_usd: Decimal = Decimal(0)
    average_entry_price: Decimal = Decimal(0)
    _seen_fills: set[UUID] = field(default_factory=set)

    @property
    def seen_fill_ids(self) -> frozenset[UUID]:
        """Fill identities already applied, exposed read-only for persistence."""
        return frozenset(self._seen_fills)

    def restore_state(
        self,
        *,
        cash_usd: Decimal,
        btc: Decimal,
        realized_pnl_usd: Decimal,
        average_entry_price: Decimal,
        seen_fill_ids: Iterable[UUID],
    ) -> None:
        """Restore a validated account snapshot before processing new fills."""
        if self._seen_fills:
            raise ValueError("cannot restore an account that has processed fills")
        if cash_usd < 0 or btc < 0 or average_entry_price < 0:
            raise ValueError("checkpoint contains a negative account balance")
        if btc == 0 and average_entry_price != 0:
            raise ValueError("checkpoint contains an entry price without BTC")
        self.cash_usd = cash_usd
        self.btc = btc
        self.realized_pnl_usd = realized_pnl_usd
        self.average_entry_price = average_entry_price
        self._seen_fills = set(seen_fill_ids)

    def apply_fill(self, fill: Fill) -> bool:
        """Apply exactly once. Returns false for a duplicate fill."""
        if fill.order_id in self._seen_fills:
            return False
        notional = fill.quantity * fill.price
        if fill.side is IntentSide.BUY:
            prior_cost = self.average_entry_price * self.btc
            self.cash_usd -= notional + fill.fee
            self.btc += fill.quantity
            self.average_entry_price = (prior_cost + notional + fill.fee) / self.btc
        elif fill.side is IntentSide.SELL:
            if fill.quantity > self.btc:
                raise ValueError("spot account cannot sell more BTC than it holds")
            self.cash_usd += notional - fill.fee
            self.realized_pnl_usd += (
                fill.price - self.average_entry_price
            ) * fill.quantity - fill.fee
            self.btc -= fill.quantity
            if self.btc == 0:
                self.average_entry_price = Decimal(0)
        else:
            raise ValueError("invalid fill side")
        self._seen_fills.add(fill.order_id)
        return True

    def equity(self, mark: Decimal) -> Decimal:
        return self.cash_usd + self.btc * mark
