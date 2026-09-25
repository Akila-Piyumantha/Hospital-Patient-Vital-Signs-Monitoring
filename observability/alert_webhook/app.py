"""Tiny Alertmanager webhook receiver: turns alerts into structured log lines.

Alertmanager POSTs grouped alerts here. Each alert is written as one JSON line to
stdout (visible in ``docker compose logs alert-webhook``) and appended to
``ALERT_LOG`` (default ``/data/alerts/alerts.jsonl`` -> ``./data/alerts/`` on the
host), which doubles as the evidence trail for the report/demo. Standard library
only, so the image needs no dependencies.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

ALERT_LOG = os.environ.get("ALERT_LOG", "/data/alerts/alerts.jsonl")
PORT = int(os.environ.get("PORT", "5001"))


def now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def emit(record: dict) -> None:
    line = json.dumps(record, default=str)
    print(line, flush=True)
    try:
        os.makedirs(os.path.dirname(ALERT_LOG), exist_ok=True)
        with open(ALERT_LOG, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    except OSError as exc:  # never let a full disk break alert delivery
        print(json.dumps({"level": "error", "event": "alert_log_write_failed", "error": str(exc)}))


def flatten(payload: dict) -> list[dict]:
    """One log record per alert in an Alertmanager webhook payload."""
    records = []
    for alert in payload.get("alerts", []):
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        status = alert.get("status", payload.get("status", "unknown"))
        records.append(
            {
                "ts": now(),
                "level": "error" if status == "firing" else "info",
                "service": "alert-webhook",
                "stage": "observability",
                "event": "alert_firing" if status == "firing" else "alert_resolved",
                "alertname": labels.get("alertname"),
                "severity": labels.get("severity"),
                "summary": annotations.get("summary"),
                "description": annotations.get("description"),
                "labels": labels,
                "starts_at": alert.get("startsAt"),
                "ends_at": alert.get("endsAt"),
            }
        )
    return records


class Handler(BaseHTTPRequestHandler):
    def _reply(self, code: int, body: bytes = b"ok") -> None:
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        self._reply(200 if self.path in ("/health", "/") else 404)

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        try:
            payload = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self._reply(400, b"invalid json")
            return
        for record in flatten(payload):
            emit(record)
        self._reply(200)

    def log_message(self, *args) -> None:  # silence default access log
        pass


if __name__ == "__main__":
    print(
        json.dumps(
            {
                "ts": now(),
                "level": "info",
                "service": "alert-webhook",
                "stage": "observability",
                "event": "started",
                "port": PORT,
            }
        ),
        flush=True,
    )
    try:
        HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)
