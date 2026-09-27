"""Sinks of the streaming job (task B6): Postgres, Parquet lake, DLQ.

Every write is idempotent, because ``foreachBatch`` is at-least-once: after a crash Spark
re-runs the last micro-batch (same ``batch_id``, same rows) from the checkpoint.

=================  =================================================================
target             how a replay stays harmless
=================  =================================================================
vitals_window      upsert on (patient_id, window_start) - aggregates are overwritten
patient_status     upsert on patient_id, guarded by ``last_reading_at`` (late data never
                   overwrites newer state)
alerts             deterministic uuid5 ids + one-open-alert unique index
dlq_events         unique (kafka_partition, kafka_offset)
Parquet lake       files are named after the batch id; a replay replaces them
vitals.dlq topic   at-least-once (a replay may re-publish; consumers dedupe on offset)
=================  =================================================================

Postgres and DLQ writes run on the driver, over rows whose number is bounded by the ward
(one summary per patient, ~5 open windows per patient) or by ``maxOffsetsPerTrigger``
(rejected records), never by the raw message rate. Each Spark action costs ~1 s of fixed
overhead in local mode, so a micro-batch runs only two: one aggregation that returns
everything the driver needs, and the Parquet write (the only per-reading sink).
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_FILE = Path(__file__).resolve().parents[1] / "sql" / "02_speed_layer.sql"

LAKE_COLUMNS = (
    "event_id",
    "patient_id",
    "heart_rate",
    "spo2",
    "systolic_bp",
    "diastolic_bp",
    "temperature",
    "timestamp",
    "event_time",
    "sim_day",
    "map",
    "pulse_pressure",
    "news_score",
    "kafka_partition",
    "kafka_offset",
)

WINDOW_COLUMNS = (
    "patient_id",
    "window_start",
    "window_end",
    "sim_day",
    "n_readings",
    *[
        f"{agg}_{v}"
        for v in ("heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature")
        for agg in ("avg", "min", "max")
    ],
    "news_score",
)


# ------------------------------------------------------------------------------ helpers
def utc(value: Any) -> Any:
    """Spark hands timestamps to Python as naive *local* datetimes; make them aware UTC."""
    if isinstance(value, datetime) and value.tzinfo is None:
        return datetime.fromtimestamp(value.timestamp(), tz=UTC)
    return value


@contextmanager
def connect(pg: dict) -> Iterator[Any]:
    """One transaction: commit on success, rollback on error, always close."""
    import psycopg2

    conn = psycopg2.connect(connect_timeout=10, **pg)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_schema(pg: dict, schema_file: Path = SCHEMA_FILE) -> None:
    """Create the speed-layer tables if missing (idempotent DDL)."""
    with connect(pg) as conn, conn.cursor() as cur:
        cur.execute(schema_file.read_text(encoding="utf-8"))


def upsert_sql(
    table: str, columns: Sequence[str], conflict: Sequence[str], update: Sequence[str]
) -> str:
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in update)
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict)}) DO UPDATE SET {sets}, updated_at = now()"
    )


def upsert_rows(cur: Any, sql: str, columns: Sequence[str], rows: Iterable[Any]) -> int:
    """Bulk upsert with ``execute_values``; ``rows`` are mappings (Spark Rows or dicts)."""
    from psycopg2.extras import execute_values

    batch = [tuple(utc(row[c]) for c in columns) for row in rows]
    if batch:
        execute_values(cur, sql, batch, page_size=500)
    return len(batch)


# ---------------------------------------------------------------------------------- DLQ
DLQ_COLUMNS = ("reason", "payload", "failed_at", "patient_id", "kafka_partition", "kafka_offset")
DLQ_INSERT_SQL = (
    f"INSERT INTO dlq_events ({', '.join(DLQ_COLUMNS)}) VALUES %s "
    "ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING"
)


def write_dlq(cur: Any, producer: Any, topic: str, rejected: list[dict]) -> None:
    """Rejected records -> Kafka ``vitals.dlq`` (contract 4.1 envelope) and ``dlq_events``.

    ``rejected`` items: patient_id, rejection_reason, raw_payload, kafka_partition, kafka_offset.
    Written from the driver: at most ``maxOffsetsPerTrigger`` rows per batch, usually 0-3.
    """
    import json

    now = datetime.now(UTC)
    failed_at = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
    rows = []
    for r in rejected:
        envelope = {
            "reason": r["rejection_reason"],
            "raw_payload": r["raw_payload"],
            "failed_at": failed_at,
        }
        key = r["patient_id"].encode() if r["patient_id"] else None
        producer.produce(topic, key=key, value=json.dumps(envelope).encode())
        rows.append(
            {
                "reason": r["rejection_reason"],
                "payload": r["raw_payload"],
                "failed_at": now,
                "patient_id": r["patient_id"],
                "kafka_partition": r["kafka_partition"],
                "kafka_offset": r["kafka_offset"],
            }
        )
    upsert_rows(cur, DLQ_INSERT_SQL, DLQ_COLUMNS, rows)
    remaining = producer.flush(10)
    if remaining:
        raise RuntimeError(f"{remaining} DLQ messages not delivered to Kafka")


# --------------------------------------------------------------------------------- lake
def write_lake(df: Any, lake_dir: str, staging_dir: str, batch_id: int, run_tag: str) -> int:
    """Append one micro-batch to ``<lake_dir>/sim_day=N/`` idempotently; returns files written.

    Spark writes the batch to a staging folder; its files are then renamed (atomic on one
    filesystem) to ``part-<run_tag>-b<batch_id>-NNN.parquet``. A replayed batch first deletes
    its own earlier files, so re-running batch 42 can never leave two copies of its rows.
    ``run_tag`` identifies the checkpoint (stable across restarts, new for a fresh checkpoint),
    so batch ids restarting at 0 after a checkpoint reset never overwrite older files.
    """
    staging = Path(staging_dir).resolve() / f"{run_tag}-batch_id={batch_id}"
    shutil.rmtree(staging, ignore_errors=True)
    # plain path, not Path.as_uri(): as_uri() percent-encodes "=" and Spark would write to
    # a folder literally named "batch_id%3D<n>"
    df.repartition("sim_day").write.mode("overwrite").partitionBy("sim_day").parquet(str(staging))
    prefix = f"part-{run_tag}-b{batch_id:010d}-"
    written = 0
    for day_dir in sorted(staging.glob("sim_day=*")):
        target = Path(lake_dir) / day_dir.name
        target.mkdir(parents=True, exist_ok=True)
        for old in target.glob(prefix + "*"):
            old.unlink()
        for k, part in enumerate(sorted(day_dir.glob("part-*.parquet"))):
            os.replace(part, target / f"{prefix}{k:03d}.parquet")
            written += 1
    shutil.rmtree(staging, ignore_errors=True)
    return written


# ------------------------------------------------------------------------ vitals_window
WINDOW_UPSERT_SQL = upsert_sql(
    "vitals_window",
    WINDOW_COLUMNS,
    ("patient_id", "window_start"),
    [c for c in WINDOW_COLUMNS if c not in ("patient_id", "window_start")],
)

RECENT_WINDOWS_SQL = """
SELECT patient_id, window_start, window_end, avg_heart_rate, avg_spo2, avg_systolic_bp, news_score
FROM (
    SELECT w.*, row_number() OVER (PARTITION BY patient_id ORDER BY window_start DESC) AS rn
    FROM vitals_window w
    WHERE patient_id = ANY(%s) AND n_readings >= %s
) t
WHERE rn <= %s
ORDER BY patient_id, window_start
"""


def fetch_recent_windows(
    cur: Any, patient_ids: list[str], min_readings: int, limit: int
) -> dict[str, list[tuple]]:
    cur.execute(RECENT_WINDOWS_SQL, (patient_ids, min_readings, limit))
    out: dict[str, list[tuple]] = {}
    for row in cur.fetchall():
        out.setdefault(row[0], []).append(row[1:])
    return out


def update_trends(cur: Any, results: dict[str, Any]) -> None:
    """Store slopes on the newest window and trend/sustained state on ``patient_status``."""
    for patient_id, res in results.items():
        cur.execute(
            "UPDATE vitals_window SET trend_slope_hr = %s, trend_slope_spo2 = %s, "
            "trend_slope_sbp = %s WHERE patient_id = %s AND window_start = %s",
            (
                _round(res.slopes["heart_rate"]),
                _round(res.slopes["spo2"]),
                _round(res.slopes["systolic_bp"]),
                patient_id,
                res.latest.window_start,
            ),
        )
        cur.execute(
            "INSERT INTO patient_status (patient_id, trend_flag, sustained_abnormal_windows) "
            "VALUES (%s, %s, %s) ON CONFLICT (patient_id) DO UPDATE SET "
            "trend_flag = EXCLUDED.trend_flag, "
            "sustained_abnormal_windows = EXCLUDED.sustained_abnormal_windows, updated_at = now()",
            (patient_id, res.trend_flag, res.sustained_windows),
        )


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 3)


# ----------------------------------------------------------------------- patient_status
STATUS_COLUMNS = (
    "patient_id",
    "last_reading_at",
    "sim_day",
    "heart_rate",
    "spo2",
    "systolic_bp",
    "diastolic_bp",
    "temperature",
    "map",
    "pulse_pressure",
    "news_score",
    "max_vital_score",
    "lab_risk_points",
    "lab_as_of_sim_day",
    "total_score",
    "risk_tier",
)
STATUS_UPSERT_SQL = (
    upsert_sql("patient_status", STATUS_COLUMNS, ("patient_id",), STATUS_COLUMNS[1:])
    + " WHERE patient_status.last_reading_at IS NULL"
    " OR EXCLUDED.last_reading_at >= patient_status.last_reading_at"
)


def fetch_status(cur: Any, patient_ids: list[str]) -> dict[str, dict]:
    cur.execute(
        "SELECT patient_id, last_reading_at, risk_tier, lab_risk_points, news_score "
        "FROM patient_status WHERE patient_id = ANY(%s)",
        (patient_ids,),
    )
    return {
        r[0]: {
            "last_reading_at": r[1],
            "risk_tier": r[2],
            "lab_risk_points": r[3],
            "news_score": r[4],
        }
        for r in cur.fetchall()
    }


def upsert_status(cur: Any, latest_rows: list[dict]) -> None:
    from psycopg2.extras import execute_values

    rows = [tuple(utc(r[c]) for c in STATUS_COLUMNS) for r in latest_rows]
    if rows:
        execute_values(cur, STATUS_UPSERT_SQL, rows)


# ------------------------------------------------------------------------------- alerts
def fetch_alert_state(
    cur: Any, patient_ids: list[str], reasons: Sequence[str]
) -> tuple[dict[tuple[str, str], Any], dict[tuple[str, str], datetime]]:
    """Open alerts and last resolution time per (patient, reason)."""
    from streaming.alerts import OpenAlert

    cur.execute(
        "SELECT patient_id, reason_code, alert_id, severity, value, opened_at FROM alerts "
        "WHERE resolved_at IS NULL AND patient_id = ANY(%s) AND reason_code = ANY(%s)",
        (patient_ids, list(reasons)),
    )
    open_alerts = {(r[0], r[1]): OpenAlert(str(r[2]), r[3], r[4], r[5]) for r in cur.fetchall()}
    cur.execute(
        "SELECT patient_id, reason_code, max(resolved_at) FROM alerts "
        "WHERE resolved_at IS NOT NULL AND patient_id = ANY(%s) AND reason_code = ANY(%s) "
        "GROUP BY patient_id, reason_code",
        (patient_ids, list(reasons)),
    )
    last_resolved = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    return open_alerts, last_resolved


def apply_alert_actions(cur: Any, actions: Any) -> list[dict]:
    """Execute engine actions; returns the alerts actually inserted (for metrics/logs)."""
    inserted = []
    for a in actions.opens:
        cur.execute(
            "INSERT INTO alerts (alert_id, patient_id, severity, reason_code, value, threshold, "
            "opened_at, last_seen_at) VALUES (%(alert_id)s, %(patient_id)s, %(severity)s, "
            "%(reason_code)s, %(value)s, %(threshold)s, %(opened_at)s, %(last_seen_at)s) "
            "ON CONFLICT DO NOTHING",
            {k: utc(v) for k, v in a.items()},
        )
        if cur.rowcount == 1:
            inserted.append(a)
    for t in actions.touches:
        cur.execute(
            "UPDATE alerts SET last_seen_at = greatest(last_seen_at, %(last_seen_at)s), "
            "severity = %(severity)s, value = %(value)s WHERE alert_id = %(alert_id)s",
            {k: utc(v) for k, v in t.items()},
        )
    for r in actions.resolves:
        cur.execute(
            "UPDATE alerts SET resolved_at = greatest(opened_at, %(resolved_at)s) "
            "WHERE alert_id = %(alert_id)s AND resolved_at IS NULL",
            {k: utc(v) for k, v in r.items()},
        )
    return inserted
