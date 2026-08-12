"""Reproducibility metadata for every backtest/paper experiment."""
from __future__ import annotations
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

@dataclass(frozen=True, slots=True)
class ExperimentManifest:
    dataset_id: str
    feature_version: str
    strategy_id: str
    parameters: dict[str, str]
    cost_model: dict[str, str]
    created_at: str
    @classmethod
    def create(cls, *, dataset_id: str, feature_version: str, strategy_id: str, parameters: dict[str, str], cost_model: dict[str, str]) -> "ExperimentManifest":
        return cls(dataset_id, feature_version, strategy_id, parameters, cost_model, datetime.now(UTC).isoformat())
    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2, sort_keys=True) + "\n", encoding="utf-8")
