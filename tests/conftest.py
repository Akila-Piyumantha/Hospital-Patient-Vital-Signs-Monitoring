"""Shared fixtures.

``pg_dsn`` - a throw-away Postgres database with the full schema (sql/01, 02, init.sql) for the
batch-layer and API tests. It connects to ``TEST_POSTGRES_HOST`` / ``_PORT`` / ``_USER`` /
``_PASSWORD`` (defaults: the Compose stack's Postgres on localhost:5432, user ``hospital``), creates
``hospital_test_<random>`` and drops it afterwards. Tests that use it are skipped when no Postgres
is reachable, so ``pytest`` stays green on a laptop without Docker; CI runs a Postgres service.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
SCHEMA = ("01_patients.sql", "02_speed_layer.sql", "init.sql")


def _admin_dsn() -> dict:
    return {
        "host": os.environ.get("TEST_POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("TEST_POSTGRES_PORT", "5432")),
        "user": os.environ.get("TEST_POSTGRES_USER", "hospital"),
        "password": os.environ.get("TEST_POSTGRES_PASSWORD", "hospital"),
        "dbname": os.environ.get("TEST_POSTGRES_ADMIN_DB", "postgres"),
        "connect_timeout": 3,
    }


@pytest.fixture(scope="session")
def pg_dsn():
    psycopg2 = pytest.importorskip("psycopg2")
    admin = _admin_dsn()
    try:
        conn = psycopg2.connect(**admin)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"no Postgres for integration tests ({admin['host']}:{admin['port']}): {exc}")
    name = f"hospital_test_{uuid.uuid4().hex[:8]}"
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE {name}")
    dsn = {k: v for k, v in admin.items() if k != "connect_timeout"} | {"dbname": name}
    try:
        with psycopg2.connect(**dsn) as db, db.cursor() as cur:
            for file in SCHEMA:
                cur.execute((SQL_DIR / file).read_text(encoding="utf-8"))
        yield dsn
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
        conn.close()


@pytest.fixture()
def pg_clean(pg_dsn):
    """``pg_dsn`` with every table emptied before the test."""
    import psycopg2

    with psycopg2.connect(**pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT string_agg(quote_ident(tablename), ', ') FROM pg_tables "
            "WHERE schemaname = 'public'"
        )
        tables = cur.fetchone()[0]
        if tables:
            cur.execute(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")
    conn.close()
    return pg_dsn
