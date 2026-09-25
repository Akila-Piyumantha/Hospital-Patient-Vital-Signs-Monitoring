"""Structured JSON logging (contract 4.8 in PROJECT_PLAN.md).

One JSON object per line on stdout, always carrying::

    ts, level, service, stage, event, run_id   (+ any event-specific fields)

``stage`` is one of ingestion | processing | storage | serving | orchestration |
observability so logs can be filtered per pipeline stage in ``docker compose logs``.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

_RESERVED = {"ts", "level", "service", "stage", "event", "run_id"}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, run_id: str, default_stage: str | None = None) -> None:
        super().__init__()
        self.service = service
        self.run_id = run_id
        self.default_stage = default_stage  # for third-party loggers that set no ``stage``

    def format(self, record: logging.LogRecord) -> str:
        dt = datetime.fromtimestamp(record.created, tz=UTC)
        payload: dict[str, Any] = {
            "ts": dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z",
            "level": record.levelname.lower(),
            "service": self.service,
            "stage": getattr(record, "stage", self.default_stage),
            "event": record.getMessage(),
            "run_id": self.run_id,
        }
        for key, value in getattr(record, "fields", {}).items():
            payload[key if key not in _RESERVED else f"field_{key}"] = value
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class StageLogger:
    """Thin wrapper: ``log.info("event_name", rows=10, duration_ms=12)``."""

    def __init__(self, logger: logging.Logger, stage: str, base: dict[str, Any]) -> None:
        self._logger = logger
        self._stage = stage
        self._base = base

    def _log(self, level: int, event: str, exc_info: bool, fields: dict[str, Any]) -> None:
        if self._logger.isEnabledFor(level):
            self._logger.log(
                level,
                event,
                exc_info=exc_info,
                extra={"stage": self._stage, "fields": {**self._base, **fields}},
            )

    def debug(self, event: str, **fields: Any) -> None:
        self._log(logging.DEBUG, event, False, fields)

    def info(self, event: str, **fields: Any) -> None:
        self._log(logging.INFO, event, False, fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._log(logging.WARNING, event, False, fields)

    def error(self, event: str, *, exc_info: bool = False, **fields: Any) -> None:
        self._log(logging.ERROR, event, exc_info, fields)

    def bind(self, **fields: Any) -> StageLogger:
        return StageLogger(self._logger, self._stage, {**self._base, **fields})


def configure_logging(
    service: str,
    level: str = "INFO",
    run_id: str | None = None,
    stream: Any = None,
    default_stage: str | None = None,
) -> str:
    """Install the JSON handler on the root logger; returns the run id in use.

    ``stream`` defaults to stdout (what ``docker compose logs`` collects); dry-run
    modes that print data on stdout pass ``sys.stderr`` to keep the two apart.
    ``default_stage`` labels records from libraries (e.g. librdkafka) that know no stage.
    """
    run_id = run_id or uuid.uuid4().hex[:8]
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter(service, run_id, default_stage))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return run_id


def get_logger(name: str, stage: str, **base: Any) -> StageLogger:
    return StageLogger(logging.getLogger(name), stage, base)
