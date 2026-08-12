from datetime import UTC, datetime
from decimal import Decimal
from tbot.execution.simulator import SimulatedVenue, SimulatorCosts
from tbot.monitoring.status import render
from tbot.paper import PaperEngine
from tbot.portfolio.account import PaperAccount
from tbot.risk.limits import BasicRiskManager, RiskLimits
from tbot.strategy.momentum import MomentumStrategy

def test_status_names_paper_mode() -> None:
    account=PaperAccount(Decimal("10")); engine=PaperEngine(MomentumStrategy(quantity=Decimal(".01")),BasicRiskManager(account,RiskLimits(Decimal("1"),Decimal("100"),Decimal("1"))),SimulatedVenue(SimulatorCosts(Decimal("0"),Decimal("0"))),account)
    assert "simulated execution" in render(engine,Decimal("100"))
