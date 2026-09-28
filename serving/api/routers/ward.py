"""``/api/ward/summary`` - the real-time ward picture (the brief's required output).

It is the serving layer's Lambda merge in one response: tier counts, alerts, vitals and
freshness come from the speed layer's tables; lab points (already folded into the tiers by the
speed layer), the latest report day and the last reconciliation come from the batch layer.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends

from common.config import env_float, env_str
from serving.api.db import Database, get_db
from serving.api.models import (
    AvgVitals,
    BatchStatus,
    ConcerningPatient,
    Freshness,
    WardSummary,
)

router = APIRouter(prefix="/api/ward", tags=["ward"])

TIERS = ("LOW", "MEDIUM", "HIGH", "CRITICAL")
SEVERITIES = ("MEDIUM", "HIGH", "CRITICAL")


def max_data_age() -> float:
    return env_float("HEALTH_MAX_DATA_AGE_SECONDS", 60.0)


def window_minutes() -> float:
    number, unit = env_str("STREAM_WINDOW_DURATION", "2 minutes").split()
    return float(number) * (1 / 60 if unit.startswith("second") else 1)


@router.get("/summary", response_model=WardSummary, summary="Real-time ward summary")
def ward_summary(db: Annotated[Database, Depends(get_db)]) -> WardSummary:
    """Patients by risk tier, active alerts, average vitals, readings/min, data freshness,
    batch-layer status and the patients that need attention now."""
    with db.cursor() as cur:

        def one(sql: str, params=None) -> dict:
            cur.execute(sql, params)
            return dict(cur.fetchone() or {})

        def many(sql: str, params=None) -> list[dict]:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

        total = one("SELECT count(*) AS n FROM patients")["n"]
        tiers = {
            r["risk_tier"]: r["n"]
            for r in many(
                "SELECT risk_tier, count(*) AS n FROM patient_status "
                "WHERE risk_tier IS NOT NULL GROUP BY 1"
            )
        }
        alerts = {
            r["severity"]: r["n"]
            for r in many(
                "SELECT severity, count(*) AS n FROM alerts WHERE resolved_at IS NULL GROUP BY 1"
            )
        }
        vit = one(
            "SELECT count(last_reading_at) AS with_data, avg(heart_rate) AS heart_rate, "
            "avg(spo2) AS spo2, avg(systolic_bp) AS systolic_bp, "
            "avg(diastolic_bp) AS diastolic_bp, avg(temperature) AS temperature, "
            "avg(news_score) AS news_score, max(last_reading_at) AS last_reading_at, "
            "EXTRACT(EPOCH FROM now() - max(last_reading_at)) AS age, max(sim_day) AS sim_day "
            "FROM patient_status"
        )
        # each patient's newest *complete* window: readings in one window length
        readings = one(
            "SELECT sum(n_readings) AS n, count(*) AS patients FROM ("
            "  SELECT DISTINCT ON (patient_id) n_readings FROM vitals_window"
            "  WHERE window_end <= now() AND window_end > now() - interval '10 minutes'"
            "  ORDER BY patient_id, window_end DESC) t"
        )
        batch = one(
            "SELECT (SELECT max(sim_day) FROM patient_risk_report) AS latest_report_sim_day, "
            "(SELECT max(as_of_sim_day) FROM patient_lab_risk) AS latest_lab_sim_day, "
            "(SELECT max(finished_at) FROM pipeline_run_log WHERE stage = 'archive_file' "
            "   AND status = 'success') AS last_dag_success_at, "
            "(SELECT discrepancy_ratio FROM speed_batch_reconciliation "
            "   ORDER BY sim_day DESC LIMIT 1) AS last_discrepancy_ratio"
        )
        concerning = many(
            "SELECT s.patient_id, p.bed, s.risk_tier, s.total_score, s.news_score, "
            "s.lab_risk_points, s.trend_flag, "
            "(SELECT count(*) FROM alerts a WHERE a.patient_id = s.patient_id "
            "   AND a.resolved_at IS NULL) AS open_alerts "
            "FROM patient_status s JOIN patients p USING (patient_id) "
            "WHERE s.risk_tier IN ('HIGH', 'CRITICAL') OR s.trend_flag <> 'STABLE' "
            "ORDER BY CASE s.risk_tier WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 "
            "WHEN 'MEDIUM' THEN 3 ELSE 4 END, s.total_score DESC NULLS LAST, s.patient_id"
        )

    age = None if vit.get("age") is None else round(float(vit["age"]), 1)
    limit = max_data_age()
    per_min = None
    if readings.get("n") is not None:
        per_min = round(float(readings["n"]) / window_minutes(), 1)
    return WardSummary(
        generated_at=datetime.now(UTC),
        total_patients=total,
        patients_with_data=vit.get("with_data") or 0,
        patients_by_tier={t: tiers.get(t, 0) for t in TIERS},
        active_alerts=sum(alerts.values()),
        active_alerts_by_severity={s: alerts.get(s, 0) for s in SEVERITIES},
        avg_vitals=AvgVitals(
            **{
                k: None if vit.get(k) is None else round(float(vit[k]), 1)
                for k in AvgVitals.model_fields
            }
        ),
        readings_per_minute=per_min,
        freshness=Freshness(
            last_reading_at=vit.get("last_reading_at"),
            data_age_seconds=age,
            stale=age is None or age > limit,
            max_age_seconds=limit,
            current_sim_day=vit.get("sim_day"),
        ),
        batch=BatchStatus(**batch),
        concerning_patients=[ConcerningPatient(**r) for r in concerning],
    )
