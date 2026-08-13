from pathlib import Path

from tbot.config import load_paper_config
from tbot.monitoring.alerts import AlertManager


def test_example_config_is_paper_only() -> None:
    config = load_paper_config(Path("configs/paper.example.toml"))
    assert config.mode == "paper" and config.symbol == "BTC-USD"


def test_alert_manager_delivers_stale_data_alert() -> None:
    found = []
    AlertManager(found.append).stale_data(12)
    assert found[0].code == "stale_data" and found[0].severity == "critical"
