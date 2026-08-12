import json

import pytest

from tbot.historical.binance import DownloadedFile, archive_url, download_range, months_between


def test_month_range_is_inclusive() -> None:
    assert months_between("2024-11", "2025-02") == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_archive_url_has_expected_kline_shape() -> None:
    assert archive_url(kind="klines", symbol="btcusdt", interval="1m", month="2024-01").endswith("/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip")


def test_archive_url_has_expected_aggregate_trade_shape() -> None:
    assert archive_url(kind="aggTrades", symbol="BTCUSDT", month="2024-01").endswith("/BTCUSDT/BTCUSDT-aggTrades-2024-01.zip")


def test_invalid_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="start"):
        months_between("2025-02", "2025-01")


def test_download_range_writes_manifest(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr("tbot.historical.binance.download_month", lambda **_: DownloadedFile("klines", "BTCUSDT", "1m", "2024-01", "url", "hash", 1, "a", "c"))
    download_range(root=tmp_path, kind="klines", symbol="BTCUSDT", start="2024-01", end="2024-01", interval="1m")
    manifest = json.loads(next(tmp_path.rglob("manifest*.json")).read_text())
    assert manifest["files"][0]["symbol"] == "BTCUSDT"
