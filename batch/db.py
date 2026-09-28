"""Postgres helpers of the batch layer: transactions, schema, ``pipeline_run_log``."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
SCHEMA_FILES = ("init.sql",)


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


def ensure_schema(pg: dict, sql_dir: Path = SQL_DIR) -> None:
    """Run the idempotent batch-layer DDL (upgrades volumes created before ``init.sql``)."""
    with connect(pg) as conn, conn.cursor() as cur:
        for name in SCHEMA_FILES:
            cur.execute((sql_dir / name).read_text(encoding="utf-8"))


def known_patients(pg: dict) -> set[str]:
    with connect(pg) as conn, conn.cursor() as cur:
        cur.execute("SELECT patient_id FROM patients")
        return {r[0] for r in cur.fetchall()}


class RunLog:
    """Context manager writing one ``pipeline_run_log`` row per task execution.

    The row is inserted as ``running`` in its own transaction (visible while the task runs) and
    closed as ``success`` / ``failed`` - a failed task leaves a trace even if its own
    transaction was rolled back::

        with RunLog(pg, "load_lab_results", run_id, sim_day) as run:
            ...
            run.rows(rows_in=120, rows_out=118, quarantined=2)
    """

    def __init__(self, pg: dict, stage: str, run_id: str, sim_day: int | None) -> None:
        self.pg, self.stage, self.run_id, self.sim_day = pg, stage, run_id, sim_day
        self.rows_in: int | None = None
        self.rows_out: int | None = None
        self.details: dict[str, Any] = {}
        self.status = "success"
        self._id: int | None = None

    def rows(self, rows_in: int | None = None, rows_out: int | None = None, **details: Any) -> None:
        self.rows_in = rows_in if rows_in is not None else self.rows_in
        self.rows_out = rows_out if rows_out is not None else self.rows_out
        self.details.update(details)

    def skip(self, **details: Any) -> None:
        self.status = "skipped"
        self.details.update(details)

    def __enter__(self) -> RunLog:
        try:
            with connect(self.pg) as conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO pipeline_run_log (stage, run_id, sim_day, status) "
                    "VALUES (%s, %s, %s, 'running') RETURNING id",
                    (self.stage, self.run_id, self.sim_day),
                )
                self._id = cur.fetchone()[0]
        except Exception:
            self._id = None  # bookkeeping must never fail the task itself
        return self

    def __exit__(self, exc_type: Any, exc: Any, _tb: Any) -> None:
        if self._id is None:
            return
        status = "failed" if exc_type else self.status
        details = dict(self.details)
        if exc is not None:
            details["error"] = f"{exc_type.__name__}: {exc}"
        try:
            with connect(self.pg) as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE pipeline_run_log SET status = %s, rows_in = %s, rows_out = %s, "
                    "details = %s, finished_at = now() WHERE id = %s",
                    (
                        status,
                        self.rows_in,
                        self.rows_out,
                        json.dumps(details, default=str),
                        self._id,
                    ),
                )
        except Exception:
            pass
