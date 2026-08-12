"""Human-friendly Rich logs and structured JSON logs from the same event API."""
from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from rich.logging import RichHandler

LogFormat = Literal["rich", "json"]


class JsonEventFormatter(logging.Formatter):
    """One machine-readable event per line, suitable for collectors and replay."""

    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "event", record.getMessage())
        fields = getattr(record, "fields", {})
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "event": event,
            **fields,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, sort_keys=True)


def configure_logging(
    *, level: str | None = None, log_format: LogFormat | None = None, log_file: Path | None = None
) -> logging.Logger:
    """Configure `tbot` once; never attach duplicate handlers on re-entry."""
    logger = logging.getLogger("tbot")
    if level is None and log_format is None and log_file is None and logger.handlers:
        return logger
    level = level or "INFO"
    log_format = log_format or "rich"
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(level.upper())

    if log_format == "json":
        console = logging.StreamHandler(sys.stderr)
        console.setFormatter(JsonEventFormatter())
    else:
        console = RichHandler(rich_tracebacks=True, show_path=False, markup=False)
        console.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(JsonEventFormatter())
        logger.addHandler(file_handler)
    return logger


def log_event(logger: logging.Logger, event: str, *, level: int = logging.INFO, **fields: Any) -> None:
    """Emit a semantic event. Do not pass credentials or raw account identifiers."""
    logger.log(level, event, extra={"event": event, "fields": fields})
