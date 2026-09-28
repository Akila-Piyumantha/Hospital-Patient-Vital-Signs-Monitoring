"""``/api/patients`` - current status per patient (speed layer) merged with lab risk (batch)."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from serving.api.db import Database, get_db
from serving.api.models import (
    Alert,
    LabResult,
    Page,
    PatientDetail,
    PatientSummary,
    PatientVitals,
    ReportEntry,
    Tier,
    VitalsWindow,
)
from serving.api.routers.alerts import ALERT_SELECT

router = APIRouter(prefix="/api/patients", tags=["patients"])

TIER_SORT = (
    "CASE s.risk_tier WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 "
    "WHEN 'LOW' THEN 4 ELSE 5 END"
)

PATIENT_SELECT = """
SELECT p.patient_id, p.name, p.age, p.sex, p.ward, p.bed, p.comorbidity, p.admitted_at,
       p.baseline_hr, p.baseline_spo2, p.baseline_sbp, p.baseline_dbp, p.baseline_temp,
       s.last_reading_at, s.sim_day, s.heart_rate, s.spo2, s.systolic_bp, s.diastolic_bp,
       s.temperature, s.map, s.news_score, s.lab_risk_points, s.lab_as_of_sim_day,
       s.total_score, s.risk_tier, s.trend_flag, s.sustained_abnormal_windows,
       coalesce(lr.abnormal_tests, '{}') AS abnormal_tests,
       (SELECT count(*) FROM alerts a
         WHERE a.patient_id = p.patient_id AND a.resolved_at IS NULL) AS open_alerts
FROM patients p
LEFT JOIN patient_status s USING (patient_id)
LEFT JOIN LATERAL (
    SELECT abnormal_tests FROM patient_lab_risk r
    WHERE r.patient_id = p.patient_id ORDER BY as_of_sim_day DESC LIMIT 1
) lr ON true
"""

FILTERS = (
    "WHERE (%(tier)s::text IS NULL OR s.risk_tier = %(tier)s) "
    "AND (%(ward)s::text IS NULL OR p.ward = %(ward)s)"
)


@router.get("", response_model=Page[PatientSummary], summary="Current status of all patients")
def list_patients(
    db: Annotated[Database, Depends(get_db)],
    risk_tier: Tier | None = None,
    ward: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> Page[PatientSummary]:
    """Most at risk first (tier, then total score). Includes lab points and the trend flag."""
    params = {"tier": risk_tier, "ward": ward, "limit": limit, "offset": offset}
    total = db.scalar(
        f"SELECT count(*) FROM patients p LEFT JOIN patient_status s USING (patient_id) {FILTERS}",
        params,
    )
    rows = db.all(
        f"{PATIENT_SELECT} {FILTERS} "
        f"ORDER BY {TIER_SORT}, s.total_score DESC NULLS LAST, p.patient_id "
        "LIMIT %(limit)s OFFSET %(offset)s",
        params,
    )
    return Page[PatientSummary](
        items=[PatientSummary(**r) for r in rows], total=total or 0, limit=limit, offset=offset
    )


@router.get("/{patient_id}", response_model=PatientDetail, summary="One patient in detail")
def get_patient(patient_id: str, db: Annotated[Database, Depends(get_db)]) -> PatientDetail:
    """Status + open alerts + labs of the latest lab file + the latest daily report entry."""
    row = db.one(f"{PATIENT_SELECT} WHERE p.patient_id = %(pid)s", {"pid": patient_id})
    if row is None:
        raise HTTPException(status_code=404, detail=f"unknown patient {patient_id}")
    alerts = db.all(
        f"{ALERT_SELECT} WHERE patient_id = %s AND resolved_at IS NULL ORDER BY opened_at DESC",
        (patient_id,),
    )
    labs = db.all(
        "SELECT sim_day, test_type, result_value, ref_low, ref_high, abnormal_flag, collected_at "
        "FROM lab_results WHERE patient_id = %(pid)s AND sim_day = "
        "(SELECT max(sim_day) FROM lab_results WHERE patient_id = %(pid)s) ORDER BY test_type",
        {"pid": patient_id},
    )
    report = db.one(
        "SELECT * FROM patient_risk_report WHERE patient_id = %s ORDER BY sim_day DESC LIMIT 1",
        (patient_id,),
    )
    baseline = {
        k.removeprefix("baseline_"): float(row.pop(k))
        for k in list(row)
        if k.startswith("baseline_") and row[k] is not None
    }
    return PatientDetail(
        **row,
        baseline=baseline,
        open_alert_list=[Alert(**a) for a in alerts],
        latest_labs=[LabResult(**lab) for lab in labs],
        latest_report=ReportEntry(**report) if report else None,
    )


@router.get(
    "/{patient_id}/vitals", response_model=PatientVitals, summary="Recent windows for charts"
)
def patient_vitals(
    patient_id: str,
    db: Annotated[Database, Depends(get_db)],
    minutes: Annotated[int, Query(ge=1, le=1440)] = 10,
) -> PatientVitals:
    """Speed-layer windows (2 min, sliding 30 s) ending in the last ``minutes`` of this
    patient's data - anchored at the newest window, so charts still show the latest picture
    when the stream is paused."""
    if db.scalar("SELECT 1 FROM patients WHERE patient_id = %s", (patient_id,)) is None:
        raise HTTPException(status_code=404, detail=f"unknown patient {patient_id}")
    rows = db.all(
        "SELECT window_start, window_end, n_readings, avg_heart_rate, min_heart_rate, "
        "max_heart_rate, avg_spo2, min_spo2, max_spo2, avg_systolic_bp, min_systolic_bp, "
        "max_systolic_bp, avg_diastolic_bp, avg_temperature, max_temperature, news_score, "
        "trend_slope_hr, trend_slope_spo2, trend_slope_sbp FROM vitals_window "
        "WHERE patient_id = %(pid)s AND window_end >= "
        "(SELECT max(window_end) FROM vitals_window WHERE patient_id = %(pid)s) "
        "- make_interval(mins => %(minutes)s) ORDER BY window_start",
        {"pid": patient_id, "minutes": minutes},
    )
    return PatientVitals(
        patient_id=patient_id, minutes=minutes, windows=[VitalsWindow(**r) for r in rows]
    )
