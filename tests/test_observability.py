import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.request import urlopen

from tbot.monitoring.alerts import AlertManager
from tbot.monitoring.logging import configure_logging, log_event
from tbot.monitoring.metrics import Metrics
from tbot.monitoring.server import Health, ObservabilityServer


def test_alerts_are_deduplicated_within_cooldown() -> None:
    delivered = []
    alerts = AlertManager(delivered.append, cooldown_seconds=60)
    now = datetime(2026, 8, 12, tzinfo=UTC)
    assert alerts.emit("critical", "stale", "stale", now=now)
    assert not alerts.emit("critical", "stale", "stale", now=now + timedelta(seconds=1))
    assert alerts.emit("critical", "stale", "stale", now=now + timedelta(seconds=60))
    assert len(delivered) == 2


def test_metrics_have_prometheus_text() -> None:
    metrics = Metrics()
    metrics.increment("feed.trade", 2)
    metrics.set_gauge("equity.usd", Decimal("100.5"))
    exposition = metrics.prometheus()
    assert "tbot_feed_trade_total 2" in exposition
    assert "tbot_equity_usd 100.5" in exposition


def test_observability_server_serves_health_status_and_metrics() -> None:
    metrics = Metrics()
    metrics.increment("feed.trade")
    server = ObservabilityServer(
        metrics=metrics,
        health=lambda: Health(True, True, {"mode": "paper"}),
        status=lambda: {"account": {"cash": "10"}},
    )
    port = server.start(port=0)
    try:
        with urlopen(f"http://127.0.0.1:{port}/readyz") as response:
            assert json.loads(response.read())["mode"] == "paper"
        with urlopen(f"http://127.0.0.1:{port}/metrics") as response:
            assert b"tbot_feed_trade_total 1" in response.read()
    finally:
        server.stop()


def test_json_audit_handler_survives_reentry(tmp_path) -> None:
    path = tmp_path / "audit.jsonl"
    logger = configure_logging(log_format="json", log_file=path)
    assert configure_logging() is logger
    log_event(logger, "test_event", value=1)
    assert json.loads(path.read_text())["event"] == "test_event"
    configure_logging(level="WARNING", log_format="rich")
