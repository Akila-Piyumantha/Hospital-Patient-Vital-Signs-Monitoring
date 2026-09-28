"""``/api/reports`` - the daily consolidated risk report (batch layer) and pipeline runs."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse

from common.config import env_str
from serving.api.db import Database, get_db
from serving.api.models import PipelineRun, ReportDay, ReportEntry, RiskReport

router = APIRouter(tags=["reports"])

TIERS = ("LOW", "MEDIUM", "HIGH", "CRITICAL")


def reports_dir() -> Path:
    return Path(env_str("REPORTS_DIR", "data/reports"))


def html_path(day: int) -> Path:
    return reports_dir() / f"risk_report_day_{day:03d}.html"


def _report(db: Database, day: int) -> RiskReport:
    rows = db.all("SELECT * FROM patient_risk_report WHERE sim_day = %s ORDER BY rank", (day,))
    if not rows:
        raise HTTPException(status_code=404, detail=f"no risk report for sim day {day}")
    ratio = db.scalar(
        "SELECT discrepancy_ratio FROM speed_batch_reconciliation WHERE sim_day = %s", (day - 1,)
    )
    entries = [ReportEntry(**r) for r in rows]
    return RiskReport(
        sim_day=day,
        vitals_sim_day=day - 1,
        generated_at=max(r["generated_at"] for r in rows),
        discrepancy_ratio=ratio,
        tier_counts={t: sum(1 for e in entries if e.risk_after_labs == t) for t in TIERS},
        tier_changes=sum(1 for e in entries if e.tier_change != "SAME"),
        html_url=f"/api/reports/risk/{day}/html" if html_path(day).is_file() else None,
        patients=entries,
    )


@router.get("/api/reports/risk", response_model=list[ReportDay], summary="Available report days")
def list_reports(
    db: Annotated[Database, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=365)] = 30,
) -> list[ReportDay]:
    rows = db.all(
        "SELECT sim_day, count(*) AS patients, "
        "count(*) FILTER (WHERE tier_change <> 'SAME') AS tier_changes, "
        "max(generated_at) AS generated_at FROM patient_risk_report "
        "GROUP BY sim_day ORDER BY sim_day DESC LIMIT %s",
        (limit,),
    )
    return [ReportDay(**r) for r in rows]


@router.get(
    "/api/reports/risk/latest", response_model=RiskReport, summary="Latest daily risk report"
)
def latest_report(db: Annotated[Database, Depends(get_db)]) -> RiskReport:
    day = db.scalar("SELECT max(sim_day) FROM patient_risk_report")
    if day is None:
        raise HTTPException(status_code=404, detail="no risk report yet (first DAG run pending)")
    return _report(db, day)


@router.get(
    "/api/reports/risk/{sim_day}", response_model=RiskReport, summary="Risk report of a sim day"
)
def report_for_day(sim_day: int, db: Annotated[Database, Depends(get_db)]) -> RiskReport:
    """``sim_day`` = the lab file day N (vitals of day N-1, labs collected on day N-1)."""
    return _report(db, sim_day)


@router.get(
    "/api/reports/risk/{sim_day}/html",
    response_class=FileResponse,
    summary="The rendered HTML report",
)
def report_html(sim_day: int) -> FileResponse:
    path = html_path(sim_day)
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"no HTML report for sim day {sim_day}")
    return FileResponse(path, media_type="text/html")


@router.get(
    "/api/pipeline/runs", response_model=list[PipelineRun], summary="Batch pipeline run log"
)
def pipeline_runs(
    db: Annotated[Database, Depends(get_db)],
    stage: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> list[PipelineRun]:
    rows = db.all(
        "SELECT * FROM pipeline_run_log WHERE (%(stage)s::text IS NULL OR stage = %(stage)s) "
        "ORDER BY started_at DESC, id DESC LIMIT %(limit)s",
        {"stage": stage, "limit": limit},
    )
    return [PipelineRun(**r) for r in rows]
