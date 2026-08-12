"""Downloader for Binance's public daily/monthly Spot archive.

The archive is research data, not a substitute for venue-matched Coinbase
execution validation. Every downloaded file has source URL and SHA-256 recorded
in a manifest next to the dataset.
"""
from __future__ import annotations

import hashlib
import json
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

BASE_URL = "https://data.binance.vision/data/spot/monthly"
SUPPORTED_KINDS = {"klines", "aggTrades", "trades"}


@dataclass(frozen=True, slots=True)
class DownloadedFile:
    kind: str
    symbol: str
    interval: str | None
    month: str
    source_url: str
    sha256: str
    byte_count: int
    archive_path: str
    csv_path: str


def months_between(start: str, end: str) -> list[str]:
    """Inclusive YYYY-MM interval list with strict validation."""
    try:
        start_year, start_month = map(int, start.split("-"))
        end_year, end_month = map(int, end.split("-"))
    except ValueError as error:
        raise ValueError("months must have YYYY-MM form") from error
    if not 1 <= start_month <= 12 or not 1 <= end_month <= 12:
        raise ValueError("month must be between 01 and 12")
    if (start_year, start_month) > (end_year, end_month):
        raise ValueError("start must not be after end")
    result: list[str] = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        result.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return result


def archive_url(*, kind: str, symbol: str, month: str, interval: str | None = None) -> str:
    if kind not in SUPPORTED_KINDS:
        raise ValueError(f"unsupported data kind: {kind}")
    normalized_symbol = symbol.upper()
    if kind == "klines":
        if not interval:
            raise ValueError("klines require an interval")
        filename = f"{normalized_symbol}-{interval}-{month}.zip"
        return f"{BASE_URL}/klines/{normalized_symbol}/{interval}/{filename}"
    filename = f"{normalized_symbol}-{kind}-{month}.zip"
    return f"{BASE_URL}/{kind}/{normalized_symbol}/{filename}"


def _download(url: str, destination: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    temporary = destination.with_suffix(destination.suffix + ".part")
    try:
        with urlopen(url, timeout=60) as response, temporary.open("wb") as handle:
            while chunk := response.read(1 << 20):
                handle.write(chunk)
                digest.update(chunk)
                byte_count += len(chunk)
    except HTTPError as error:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"archive unavailable ({error.code}): {url}") from error
    temporary.replace(destination)
    return digest.hexdigest(), byte_count


def download_month(*, root: Path, kind: str, symbol: str, month: str, interval: str | None = None) -> DownloadedFile:
    """Download and extract one immutable archive, reusing an existing verified file."""
    url = archive_url(kind=kind, symbol=symbol, month=month, interval=interval)
    source_name = url.rsplit("/", 1)[-1]
    folder = root / "binance" / "spot" / kind / f"symbol={symbol.upper()}" / f"month={month}"
    folder.mkdir(parents=True, exist_ok=True)
    archive = folder / source_name
    if archive.exists():
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        byte_count = archive.stat().st_size
    else:
        digest, byte_count = _download(url, archive)
    with zipfile.ZipFile(archive) as zipped:
        csv_members = [member for member in zipped.namelist() if member.endswith(".csv")]
        if len(csv_members) != 1:
            raise RuntimeError(f"expected exactly one CSV in {archive}")
        csv_path = folder / Path(csv_members[0]).name
        if not csv_path.exists():
            zipped.extract(csv_members[0], folder)
            extracted = folder / csv_members[0]
            if extracted != csv_path:
                extracted.replace(csv_path)
    return DownloadedFile(kind, symbol.upper(), interval, month, url, digest, byte_count, str(archive), str(csv_path))


def download_range(*, root: Path, kind: str, symbol: str, start: str, end: str, interval: str | None = None) -> list[DownloadedFile]:
    files = [download_month(root=root, kind=kind, symbol=symbol, month=month, interval=interval) for month in months_between(start, end)]
    manifest = {
        "provider": "Binance public data archive",
        "market": "spot",
        "retrieved_at": datetime.now(UTC).isoformat(),
        "files": [asdict(item) for item in files],
    }
    directory = root / "binance" / "spot" / kind / f"symbol={symbol.upper()}"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"manifest-{start}-to-{end}.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return files
