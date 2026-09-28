"""``/api/alerts`` - patient alerts raised by the speed layer (open -> resolved lifecycle)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query

from serving.api.db import Database, get_db
from serving.api.models import Alert, Page, Severity

router = APIRouter(prefix="/api/alerts", tags=["alerts"])

ALERT_SELECT = (
    "SELECT alert_id::text AS alert_id, patient_id, severity, reason_code, value, threshold, "
    "opened_at, last_seen_at, resolved_at, "
    "CASE WHEN resolved_at IS NULL THEN 'open' ELSE 'resolved' END AS status FROM alerts"
)

WHERE = (
    "WHERE (%(status)s = 'all' OR (%(status)s = 'open') = (resolved_at IS NULL)) "
    "AND (%(severity)s::text IS NULL OR severity = %(severity)s) "
    "AND (%(pid)s::text IS NULL OR patient_id = %(pid)s) "
    "AND (%(since)s::timestamptz IS NULL OR opened_at >= %(since)s)"
)
SEVERITY_SORT = "CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END"


@router.get("", response_model=Page[Alert], summary="Alert list")
def list_alerts(
    db: Annotated[Database, Depends(get_db)],
    status: Literal["open", "resolved", "all"] = "open",
    severity: Severity | None = None,
    patient_id: str | None = None,
    since: Annotated[
        datetime | None, Query(description="only alerts opened at/after this time")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> Page[Alert]:
    """Open alerts are sorted by severity, then newest first; others newest first."""
    params = {
        "status": status,
        "severity": severity,
        "pid": patient_id,
        "since": since,
        "limit": limit,
        "offset": offset,
    }
    order = f"{SEVERITY_SORT}, opened_at DESC" if status == "open" else "opened_at DESC"
    total = db.scalar(f"SELECT count(*) FROM alerts {WHERE}", params)
    rows = db.all(
        f"{ALERT_SELECT} {WHERE} ORDER BY {order}, alert_id LIMIT %(limit)s OFFSET %(offset)s",
        params,
    )
    return Page[Alert](
        items=[Alert(**r) for r in rows], total=total or 0, limit=limit, offset=offset
    )
