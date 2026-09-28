"""Serving API entry point: ``uvicorn serving.api.main:app`` (owner: Member C, task C6).

Endpoints (contract 4.6; interactive docs at ``/docs``, spec at ``/openapi.json``):

    GET /api/ward/summary                     real-time ward picture
    GET /api/patients[?risk_tier=&ward=]      current status per patient (paginated)
    GET /api/patients/{id}                    status, open alerts, latest labs, latest report
    GET /api/patients/{id}/vitals?minutes=10  recent windows for charts
    GET /api/alerts?status=open&severity=     alert list (paginated)
    GET /api/reports/risk[/latest|/{sim_day}] daily consolidated risk report (+ /html)
    GET /api/pipeline/runs                    batch pipeline run log
    GET /health                               DB + data freshness (503 when stale)
    GET /health/live                          process liveness (Docker healthcheck)
    GET /metrics                              Prometheus exposition
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily

from common.config import env_float, env_str
from common.logging_setup import configure_logging, get_logger
from serving.api.db import Database, DatabaseUnavailable, get_db
from serving.api.models import Health
from serving.api.routers import alerts, patients, reports, ward

log = get_logger("serving.api", "serving")

REQUEST_DURATION = Histogram(
    "api_request_duration_seconds",
    "Latency of API requests",
    ["method", "route", "status"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)


class WardCollector:
    """Gauges read from Postgres at scrape time (one cheap query, skipped if the DB is down).

    ``pipeline_last_event_age_seconds`` is also exported by the Spark job; the API's copy keeps
    ``PipelineDataStale`` working when the streaming job itself is dead.
    """

    def __init__(self, db_provider: Any) -> None:
        self.db_provider = db_provider

    def collect(self) -> Iterator[GaugeMetricFamily]:
        try:
            db = self.db_provider()
            row = db.one(
                "SELECT EXTRACT(EPOCH FROM now() - max(last_reading_at)) AS age FROM patient_status"
            )
            tiers = db.all(
                "SELECT risk_tier, count(*) AS n FROM patient_status "
                "WHERE risk_tier IS NOT NULL GROUP BY 1"
            )
            open_alerts = db.all(
                "SELECT severity, count(*) AS n FROM alerts WHERE resolved_at IS NULL GROUP BY 1"
            )
        except Exception:
            return
        if row and row["age"] is not None:
            age = GaugeMetricFamily(
                "pipeline_last_event_age_seconds",
                "Seconds since the newest reading in patient_status (API view)",
            )
            age.add_metric([], float(row["age"]))
            yield age
        by_tier = {r["risk_tier"]: r["n"] for r in tiers}
        tier_g = GaugeMetricFamily("ward_patients", "Patients per risk tier", labels=["risk_tier"])
        for tier in ward.TIERS:
            tier_g.add_metric([tier], by_tier.get(tier, 0))
        yield tier_g
        by_sev = {r["severity"]: r["n"] for r in open_alerts}
        alert_g = GaugeMetricFamily("ward_open_alerts", "Open patient alerts", labels=["severity"])
        for sev in ward.SEVERITIES:
            alert_g.add_metric([sev], by_sev.get(sev, 0))
        yield alert_g


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    db = get_db()
    if isinstance(db, Database):
        db.close()


def create_app(register_collector: bool = True) -> FastAPI:
    app = FastAPI(
        title="Hospital Vital Signs - Serving API",
        version="1.0.0",
        lifespan=lifespan,
        description=(
            "Real-time ward figures (speed layer) merged with the daily lab-driven risk report "
            "(batch layer). Scores are a simplified NEWS2 adaptation, not clinically validated."
        ),
    )
    for r in (ward.router, patients.router, alerts.router, reports.router):
        app.include_router(r)

    @app.middleware("http")
    async def observe(request: Request, call_next):
        started = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            elapsed = time.perf_counter() - started
            route = getattr(request.scope.get("route"), "path", "unmatched")
            if route != "/metrics":
                REQUEST_DURATION.labels(request.method, route, str(status)).observe(elapsed)
                log.info(
                    "request",
                    method=request.method,
                    route=route,
                    path=request.url.path,
                    status=status,
                    duration_ms=round(elapsed * 1000, 1),
                )

    @app.exception_handler(DatabaseUnavailable)
    async def db_unavailable(_request: Request, exc: DatabaseUnavailable) -> JSONResponse:
        log.error("database_unavailable", error=str(exc))
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})

    @app.get("/health", response_model=Health, tags=["ops"], summary="Liveness + freshness")
    def health(db: Annotated[Database, Depends(get_db)]) -> JSONResponse:
        """503 when Postgres is unreachable or the newest reading is older than
        ``HEALTH_MAX_DATA_AGE_SECONDS`` (default 60 s)."""
        limit = env_float("HEALTH_MAX_DATA_AGE_SECONDS", 60.0)
        try:
            age = db.scalar(
                "SELECT EXTRACT(EPOCH FROM now() - max(last_reading_at)) FROM patient_status"
            )
        except Exception as exc:
            body = Health(
                status="down", database="unavailable", max_age_seconds=limit, detail=str(exc)[:200]
            )
            return JSONResponse(status_code=503, content=body.model_dump())
        age = None if age is None else round(float(age), 1)
        if age is None or age > limit:
            detail = "no vitals processed yet" if age is None else f"newest reading {age}s old"
            body = Health(
                status="degraded",
                database="ok",
                data_age_seconds=age,
                max_age_seconds=limit,
                detail=detail,
            )
            return JSONResponse(status_code=503, content=body.model_dump())
        body = Health(status="ok", database="ok", data_age_seconds=age, max_age_seconds=limit)
        return JSONResponse(status_code=200, content=body.model_dump())

    @app.get("/health/live", tags=["ops"], summary="Process liveness only")
    def live() -> dict:
        return {"status": "alive"}

    @app.get("/metrics", tags=["ops"], summary="Prometheus metrics")
    def metrics() -> Response:
        return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    if register_collector:
        global _collector_registered
        if not _collector_registered:  # the registry is global; create_app may run twice
            REGISTRY.register(WardCollector(get_db))
            _collector_registered = True

    return app


_collector_registered = False
configure_logging("api", env_str("LOG_LEVEL", "INFO"), default_stage="serving")
app = create_app()
