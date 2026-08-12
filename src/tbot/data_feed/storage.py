"""Append-only JSONL storage used before a Parquet compaction job is introduced."""
from __future__ import annotations
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, Decimal, UUID)):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value)!r}")

class JsonlEventStore:
    def __init__(self, root: Path) -> None:
        self.root = root
    def append(self, *, stream: str, event: object, received_at: datetime) -> Path:
        if received_at.tzinfo is None:
            raise ValueError("received_at must be timezone-aware")
        path = self.root / stream / f"date={received_at:%Y-%m-%d}" / "events.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(event) if is_dataclass(event) else event
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=_json_default, sort_keys=True) + "\n")
        return path
