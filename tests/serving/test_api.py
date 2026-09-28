"""Serving API (task C8): FastAPI TestClient, with and without a (test) Postgres.

* without a database: contract routes exist, ``/health`` and data routes answer 503 when Postgres
  is down, ``/metrics`` exposes ``api_request_duration_seconds``;
* with the ``pg_clean`` test database (skipped when no Postgres is reachable): every endpoint
  against hand-written rows of the speed and batch layers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from serving.api.db import Database, get_db  # noqa: E402
from serving.api.main import app  # noqa: E402


@pytest.fixture()
def down_client():
    dead = Database(
        {
            "host": "127.0.0.1",
            "port": 1,
            "user": "x",
            "password": "x",
            "dbname": "x",
            "connect_timeout": 1,
        }
    )
    app.dependency_overrides[get_db] = lambda: dead
    yield TestClient(app)
    app.dependency_overrides.clear()


# ------------------------------------------------------------------------- no database
def test_contract_routes_are_published():
    paths = TestClient(app).get("/openapi.json").json()["paths"]
    for route in (
        "/api/ward/summary",
        "/api/patients",
        "/api/patients/{patient_id}",
        "/api/patients/{patient_id}/vitals",
        "/api/alerts",
        "/api/reports/risk/latest",
        "/api/reports/risk/{sim_day}",
        "/health",
        "/metrics",
    ):
        assert route in paths, route


def test_database_down_gives_503(down_client):
    health = down_client.get("/health")
    assert health.status_code == 503
    assert health.json()["status"] == "down" and health.json()["database"] == "unavailable"
    assert down_client.get("/api/ward/summary").status_code == 503
    assert down_client.get("/api/patients").json() == {"detail": "database unavailable"}
    assert down_client.get("/health/live").status_code == 200


def test_metrics_endpoint_records_request_latency(down_client):
    down_client.get("/health/live")
    body = down_client.get("/metrics").text
    assert "api_request_duration_seconds_bucket{" in body
    assert 'route="/health/live"' in body


def test_validation_errors_are_422():
    client = TestClient(app)
    assert client.get("/api/alerts?status=bogus").status_code == 422
    assert client.get("/api/patients?limit=0").status_code == 422
    assert client.get("/api/patients/P001/vitals?minutes=0").status_code == 422


# ----------------------------------------------------------------------- with Postgres
@pytest.fixture()
def client(pg_clean):
    seed(pg_clean)
    db = Database(pg_clean)
    app.dependency_overrides[get_db] = lambda: db
    yield TestClient(app)
    app.dependency_overrides.clear()
    db.close()


def seed(pg: dict) -> None:
    import psycopg2

    now = datetime.now(UTC)
    t = now - timedelta(seconds=5)
    with psycopg2.connect(**pg) as conn, conn.cursor() as cur:
        for i, (pid, ward) in enumerate([("P001", "W1"), ("P002", "W1"), ("P003", "W2")]):
            cur.execute(
                "INSERT INTO patients (patient_id, name, age, sex, ward, bed, baseline_hr, "
                "baseline_spo2, baseline_sbp, baseline_dbp, baseline_temp) "
                "VALUES (%s, %s, 70, 'M', %s, %s, 75, 97, 120, 80, 36.8)",
                (pid, f"Name {i}", ward, f"B{i + 1}"),
            )
        status = [
            ("P001", 125, 90, 95, 38.9, 7, 2, 9, "CRITICAL", "HR_RISING,SPO2_FALLING"),
            ("P002", 80, 97, 120, 36.8, 0, 4, 4, "MEDIUM", "STABLE"),
            ("P003", 70, 98, 125, 36.6, 0, 0, 0, "LOW", "STABLE"),
        ]
        for pid, hr, spo2, sbp, temp, news, lab, total, tier, trend in status:
            cur.execute(
                "INSERT INTO patient_status (patient_id, last_reading_at, sim_day, heart_rate, "
                "spo2, systolic_bp, diastolic_bp, temperature, news_score, max_vital_score, "
                "lab_risk_points, lab_as_of_sim_day, total_score, risk_tier, trend_flag) "
                "VALUES (%s, %s, 4, %s, %s, %s, 70, %s, %s, 3, %s, 4, %s, %s, %s)",
                (pid, t, hr, spo2, sbp, temp, news, lab, total, tier, trend),
            )
        for k in range(6):  # 2-min windows sliding 30 s, all complete
            start = now - timedelta(minutes=6) + timedelta(seconds=30 * k)
            cur.execute(
                "INSERT INTO vitals_window (patient_id, window_start, window_end, sim_day, "
                "n_readings, avg_heart_rate, avg_spo2, avg_systolic_bp, news_score) "
                "VALUES ('P001', %s, %s, 4, 60, %s, 92, 100, %s)",
                (start, start + timedelta(minutes=2), 100 + 4 * k, 3 + k // 2),
            )
        cur.execute(
            "INSERT INTO alerts (alert_id, patient_id, severity, reason_code, value, threshold, "
            "opened_at, last_seen_at, resolved_at) VALUES "
            "('00000000-0000-0000-0000-000000000001', 'P001', 'CRITICAL', 'TOTAL_SCORE_HIGH', "
            " 9, 5, %(t)s, %(t)s, NULL), "
            "('00000000-0000-0000-0000-000000000002', 'P001', 'HIGH', 'HR_CRITICAL', "
            " 135, 131, %(t)s, %(t)s, NULL), "
            "('00000000-0000-0000-0000-000000000003', 'P002', 'MEDIUM', 'WORSENING_TREND', "
            " 1, 1, %(old)s, %(old)s, %(t)s)",
            {"t": t, "old": t - timedelta(minutes=10)},
        )
        cur.execute(
            "INSERT INTO lab_results (sim_day, patient_id, test_type, result_value, ref_low, "
            "ref_high, abnormal_flag, collected_at, source_file) VALUES "
            "(4, 'P002', 'lactate', 3.4, 0.5, 2.0, 'HIGH', %(t)s, 'labs_day_004.csv'), "
            "(4, 'P002', 'wbc', 14.1, 4.0, 11.0, 'HIGH', %(t)s, 'labs_day_004.csv'), "
            "(3, 'P002', 'lactate', 1.0, 0.5, 2.0, NULL, %(t)s, 'labs_day_003.csv')",
            {"t": t},
        )
        cur.execute(
            "INSERT INTO patient_lab_risk (patient_id, lab_risk_points, abnormal_tests, "
            "as_of_sim_day) VALUES ('P002', 0, '{}', 3), "
            "('P002', 3, '{lactate:high,wbc:high}', 4), ('P001', 2, '{crp:high}', 4)"
        )
        for rank, (pid, before, after, change) in enumerate(
            [
                ("P001", "CRITICAL", "CRITICAL", "SAME"),
                ("P002", "LOW", "MEDIUM", "UP"),
                ("P003", "LOW", "LOW", "SAME"),
            ],
            start=1,
        ):
            cur.execute(
                "INSERT INTO patient_risk_report (sim_day, patient_id, rank, vitals_sim_day, "
                "vitals_source, vitals_summary, lab_summary, news_score, max_vital_score, trend, "
                "lab_points_before, lab_points_after, total_before, total_after, "
                "risk_before_labs, risk_after_labs, tier_change, abnormal_tests) VALUES "
                "(4, %s, %s, 3, 'batch', 'HR 80', 'labs', 1, 1, 'STABLE', 0, 3, 1, 4, %s, %s, "
                "%s, '{}')",
                (pid, rank, before, after, change),
            )
        cur.execute(
            "INSERT INTO speed_batch_reconciliation (sim_day, windows_compared, windows_missing, "
            "batch_readings, speed_readings, count_abs_diff, discrepancy_ratio, lake_readings, "
            "lake_duplicates) VALUES (3, 120, 0, 7200, 7150, 50, 0.0069, 3000, 4)"
        )
        cur.execute(
            "INSERT INTO pipeline_run_log (stage, run_id, sim_day, status, finished_at) "
            "VALUES ('archive_file', 'r1', 4, 'success', now())"
        )
    conn.close()


def test_ward_summary(client):
    body = client.get("/api/ward/summary").json()
    assert body["total_patients"] == 3 and body["patients_with_data"] == 3
    assert body["patients_by_tier"] == {"LOW": 1, "MEDIUM": 1, "HIGH": 0, "CRITICAL": 1}
    assert body["active_alerts"] == 2
    assert body["active_alerts_by_severity"] == {"MEDIUM": 0, "HIGH": 1, "CRITICAL": 1}
    assert body["avg_vitals"]["heart_rate"] == pytest.approx((125 + 80 + 70) / 3, abs=0.1)
    assert body["readings_per_minute"] == 30.0  # newest complete window: 60 readings / 2 min
    assert body["freshness"]["stale"] is False and body["freshness"]["current_sim_day"] == 4
    assert body["batch"]["latest_report_sim_day"] == 4
    assert body["batch"]["latest_lab_sim_day"] == 4
    assert body["batch"]["last_discrepancy_ratio"] == pytest.approx(0.0069)
    assert [p["patient_id"] for p in body["concerning_patients"]] == ["P001"]


def test_health_ok_then_stale(client, pg_clean):
    import psycopg2

    assert client.get("/health").json()["status"] == "ok"
    with psycopg2.connect(**pg_clean) as conn, conn.cursor() as cur:
        cur.execute("UPDATE patient_status SET last_reading_at = now() - interval '5 minutes'")
    conn.close()
    stale = client.get("/health")
    assert stale.status_code == 503 and stale.json()["status"] == "degraded"


def test_patients_list_sorted_filtered_paginated(client):
    body = client.get("/api/patients").json()
    assert body["total"] == 3
    assert [p["patient_id"] for p in body["items"]] == ["P001", "P002", "P003"]
    first = body["items"][0]
    assert first["open_alerts"] == 2 and first["trend_flag"] == "HR_RISING,SPO2_FALLING"
    assert body["items"][1]["abnormal_tests"] == ["lactate:high", "wbc:high"]  # newest lab day
    assert client.get("/api/patients?risk_tier=LOW").json()["total"] == 1
    assert client.get("/api/patients?ward=W2").json()["items"][0]["patient_id"] == "P003"
    page = client.get("/api/patients?limit=1&offset=1").json()
    assert page["total"] == 3 and [p["patient_id"] for p in page["items"]] == ["P002"]


def test_patient_detail(client):
    body = client.get("/api/patients/P002").json()
    assert body["risk_tier"] == "MEDIUM" and body["baseline"]["hr"] == 75
    assert [lab["test_type"] for lab in body["latest_labs"]] == ["lactate", "wbc"]
    assert all(lab["sim_day"] == 4 for lab in body["latest_labs"])
    assert body["latest_report"]["risk_after_labs"] == "MEDIUM"
    assert body["open_alert_list"] == []
    assert client.get("/api/patients/P404").status_code == 404


def test_patient_vitals_window_range(client):
    body = client.get("/api/patients/P001/vitals?minutes=1").json()
    assert len(body["windows"]) == 3  # window_end within 1 min of the newest one
    assert body["windows"][-1]["avg_heart_rate"] == 120
    assert len(client.get("/api/patients/P001/vitals?minutes=60").json()["windows"]) == 6
    assert client.get("/api/patients/P003/vitals").json()["windows"] == []
    assert client.get("/api/patients/P404/vitals").status_code == 404


def test_alerts_filters(client):
    open_ = client.get("/api/alerts").json()
    assert open_["total"] == 2
    assert [a["severity"] for a in open_["items"]] == ["CRITICAL", "HIGH"]
    assert client.get("/api/alerts?status=resolved").json()["items"][0]["patient_id"] == "P002"
    assert client.get("/api/alerts?status=all").json()["total"] == 3
    assert client.get("/api/alerts?severity=HIGH").json()["total"] == 1
    assert client.get("/api/alerts?status=all&patient_id=P002").json()["total"] == 1
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    assert client.get("/api/alerts", params={"since": future}).json()["total"] == 0


def test_reports(client):
    latest = client.get("/api/reports/risk/latest").json()
    assert latest["sim_day"] == 4 and latest["vitals_sim_day"] == 3
    assert latest["tier_changes"] == 1
    assert latest["tier_counts"] == {"LOW": 1, "MEDIUM": 1, "HIGH": 0, "CRITICAL": 1}
    assert latest["discrepancy_ratio"] == pytest.approx(0.0069)
    assert [p["patient_id"] for p in latest["patients"]] == ["P001", "P002", "P003"]
    assert client.get("/api/reports/risk/4").json()["patients"] == latest["patients"]
    assert client.get("/api/reports/risk/9").status_code == 404
    days = client.get("/api/reports/risk").json()
    assert days == [
        {"sim_day": 4, "patients": 3, "tier_changes": 1, "generated_at": days[0]["generated_at"]}
    ]
    runs = client.get("/api/pipeline/runs?stage=archive_file").json()
    assert runs[0]["status"] == "success"


def test_report_html_served_from_reports_dir(client, tmp_path, monkeypatch):
    monkeypatch.setenv("REPORTS_DIR", str(tmp_path))
    assert client.get("/api/reports/risk/4/html").status_code == 404
    (tmp_path / "risk_report_day_004.html").write_text("<html>day 4</html>")
    page = client.get("/api/reports/risk/4/html")
    assert page.status_code == 200 and "day 4" in page.text
    assert client.get("/api/reports/risk/latest").json()["html_url"] == "/api/reports/risk/4/html"
