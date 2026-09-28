"""Postgres access for the API: a small thread-safe connection pool and dict rows.

FastAPI runs the (sync) endpoint functions in a thread pool, so a ``ThreadedConnectionPool`` is
enough - no async driver needed at this scale. Every request runs read-only SQL in its own short
transaction; a broken connection is discarded instead of being returned to the pool.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from common.config import Settings, env_int


class DatabaseUnavailable(RuntimeError):
    """Raised when Postgres cannot be reached; mapped to HTTP 503."""


class Database:
    def __init__(self, dsn: dict, minconn: int = 1, maxconn: int = 8) -> None:
        self.dsn = dsn
        self.minconn, self.maxconn = minconn, maxconn
        self._pool = None
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls) -> Database:
        s = Settings.from_env()
        return cls(
            {
                "host": s.postgres_host,
                "port": s.postgres_port,
                "user": s.postgres_user,
                "password": s.postgres_password,
                "dbname": s.postgres_db,
                "connect_timeout": 5,
                "application_name": "hospital-api",
            },
            maxconn=env_int("API_DB_POOL_SIZE", 8),
        )

    def _get_pool(self) -> Any:
        import psycopg2
        from psycopg2.pool import ThreadedConnectionPool

        with self._lock:
            if self._pool is None:
                try:
                    self._pool = ThreadedConnectionPool(self.minconn, self.maxconn, **self.dsn)
                except psycopg2.Error as exc:
                    raise DatabaseUnavailable(str(exc)) from exc
            return self._pool

    @contextmanager
    def cursor(self) -> Iterator[Any]:
        import psycopg2
        from psycopg2.extras import RealDictCursor

        pool = self._get_pool()
        try:
            conn = pool.getconn()
        except psycopg2.Error as exc:
            raise DatabaseUnavailable(str(exc)) from exc
        broken = False
        try:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET LOCAL statement_timeout = 5000")
                yield cur
            conn.rollback()  # read-only: nothing to commit
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            broken = True
            raise DatabaseUnavailable(str(exc)) from exc
        except Exception:
            try:
                conn.rollback()
            except psycopg2.Error:
                broken = True
            raise
        finally:
            pool.putconn(conn, close=broken or bool(conn.closed))

    def all(self, sql: str, params: Any = None) -> list[dict]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def one(self, sql: str, params: Any = None) -> dict | None:
        rows = self.all(sql, params)
        return rows[0] if rows else None

    def scalar(self, sql: str, params: Any = None) -> Any:
        row = self.one(sql, params)
        return None if row is None else next(iter(row.values()))

    def close(self) -> None:
        with self._lock:
            if self._pool is not None:
                self._pool.closeall()
                self._pool = None


_db: Database | None = None


def get_db() -> Database:
    """FastAPI dependency (tests override it with ``app.dependency_overrides``)."""
    global _db
    if _db is None:
        _db = Database.from_env()
    return _db
