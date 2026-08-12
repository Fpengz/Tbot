"""Small dependency-free status view suitable for logs or a terminal."""
from __future__ import annotations

from decimal import Decimal

from tbot.paper import PaperEngine


def render(engine: PaperEngine, mark_price: Decimal) -> str:
    return "\n".join((
        "mode: paper (simulated execution)",
        f"cash_usd: {engine.account.cash_usd}",
        f"btc: {engine.account.btc}",
        f"equity_usd: {engine.account.equity(mark_price)}",
        f"fills: {len(engine.fills)}", f"risk_rejections: {engine.metrics.counters['risk.rejected']}",
    ))
