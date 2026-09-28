"""The DAG's task chain against a real Postgres, without Airflow (tasks C3, C7, C8).

Runs ``batch.tasks`` in DAG order with a fake task instance (XCom in a dict) and checks the
failure-handling cases of C7: replaying a day is idempotent, a corrupt file loads only its good
rows, unknown patients are quarantined, a bad-schema file is quarantined as a whole and the
report falls back to the previous labs, a missing file increments ``lab_file_missing_total``.

Needs Postgres (fixture ``pg_clean``, skipped otherwise). The Spark step is skipped by using
sim day 1 (no vitals day before it) or by the pyspark-less environment.
"""

from __future__ import annotations

import json
import random

import pytest

pytest.importorskip("psycopg2")

from batch import tasks  # noqa: E402
from common.sim_clock import SimClock  # noqa: E402
from simulators.lab_generator import corrupt_rows, generate_lab_rows, write_lab_file  # noqa: E402
from simulators.patients import build_patients  # noqa: E402

PATIENTS = build_patients(20, 42, 300.0, 4, 2)
CLOCK = SimClock(epoch=1_767_225_600.0, day_seconds=300.0)
ORDER = [
    "validate_lab_file",
    "load_lab_results",
    "compute_lab_risk",
    "batch_vitals_job",
    "build_risk_report",
    "data_quality_and_health_check",
    "archive_file",
]


class FakeTI:
    def __init__(self, task_id: str = "", store: dict | None = None) -> None:
        self.task_id = task_id
        self.try_number = 1
        self.store = store if store is not None else {}

    def xcom_pull(self, task_ids: str):
        return self.store.get(task_ids)


class FakeRun:
    def __init__(self, conf: dict, run_type: str = "manual") -> None:
        self.conf = conf
        self.run_type = run_type


@pytest.fixture()
def dirs(tmp_path, monkeypatch):
    for key, value in {
        "LANDING_DIR": str(tmp_path / "landing"),
        "STATE_DIR": str(tmp_path / "state"),
        "REPORTS_DIR": str(tmp_path / "reports"),
        "LAKE_DIR": str(tmp_path / "lake"),
        "SIM_EPOCH": str(CLOCK.epoch),
        "PUSHGATEWAY_URL": "127.0.0.1:9",  # unreachable: pushes must not fail tasks
        "BATCH_SETTLE_SECONDS": "0",
    }.items():
        monkeypatch.setenv(key, value)
    return tmp_path


@pytest.fixture()
def env(dirs, pg_clean, monkeypatch):
    import psycopg2
    from psycopg2.extras import execute_values

    for key, value in {
        "POSTGRES_HOST": pg_clean["host"],
        "POSTGRES_PORT": str(pg_clean["port"]),
        "POSTGRES_USER": pg_clean["user"],
        "POSTGRES_PASSWORD": pg_clean["password"],
        "POSTGRES_DB": pg_clean["dbname"],
    }.items():
        monkeypatch.setenv(key, value)
    with psycopg2.connect(**pg_clean) as conn, conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO patients (patient_id, name, age, sex, ward, bed, baseline_hr, "
            "baseline_spo2, baseline_sbp, baseline_dbp, baseline_temp) VALUES %s",
            [
                (p.patient_id, p.patient_id, 60, "F", "W1", f"B{i}", 75, 97, 120, 80, 36.8)
                for i, p in enumerate(PATIENTS)
            ],
        )
    conn.close()
    return dirs


def drop(env_dir, day, rows=None, columns=None):
    rows = rows if rows is not None else generate_lab_rows(day, PATIENTS, CLOCK, 42)
    kwargs = {"columns": columns} if columns else {}
    return write_lab_file(env_dir / "landing", day, rows, **kwargs)


def run_dag(day: int) -> dict:
    store: dict = {}
    ctx = {"run_id": f"test_{day}", "dag_run": FakeRun({"sim_day": day})}
    store["resolve_sim_day"] = tasks.resolve_sim_day(ti=FakeTI("resolve_sim_day", store), **ctx)
    for name in ORDER:
        store[name] = getattr(tasks, name)(ti=FakeTI(name, store), **ctx)
    return store


def query(pg, sql, *params):
    import psycopg2

    with psycopg2.connect(**pg) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    conn.close()
    return rows


def test_resolve_sim_day_sources(env):
    from datetime import UTC, datetime

    def resolve(run, **extra):
        return tasks.resolve_sim_day(ti=FakeTI(), run_id="r", dag_run=run, **extra)

    # scheduled: data_interval_end 2.9 days after the epoch -> day 3, however late the run starts
    end = datetime.fromtimestamp(CLOCK.epoch + 2.9 * CLOCK.day_seconds, tz=UTC)
    assert resolve(FakeRun({}, "scheduled"), data_interval_end=end) == 3
    assert resolve(FakeRun({"sim_day": 7}, "scheduled"), data_interval_end=end) == 7  # conf wins
    before_epoch = datetime.fromtimestamp(CLOCK.epoch - 120, tz=UTC)  # first run after start-up
    assert resolve(FakeRun({}, "scheduled"), data_interval_end=before_epoch) == 1
    assert resolve(FakeRun({}), data_interval_end=end) == CLOCK.sim_day()  # manual: now


def test_full_run_then_replay_is_idempotent(env, pg_clean):
    drop(env, 1)
    first = run_dag(1)
    assert first["validate_lab_file"]["status"] == "ok"
    assert first["batch_vitals_job"]["status"] == "skipped"  # no day 0 vitals
    assert first["build_risk_report"]["rows"] == len(PATIENTS)
    assert (env / "landing" / "processed" / "labs_day_001.csv").exists()
    assert (env / "reports" / "risk_report_day_001.html").exists()
    assert (env / "reports" / "risk_report_day_001.csv").exists()

    checks = (
        "SELECT sim_day, patient_id, test_type, result_value, ref_low, ref_high, abnormal_flag, "
        "collected_at FROM lab_results ORDER BY 1, 2, 3",
        "SELECT patient_id, lab_risk_points, abnormal_tests, as_of_sim_day "
        "FROM patient_lab_risk ORDER BY 1",
        "SELECT patient_id, rank, risk_before_labs, risk_after_labs, lab_points_after, "
        "lab_summary FROM patient_risk_report ORDER BY rank",
    )
    snapshot = [query(pg_clean, sql) for sql in checks]
    assert snapshot[0] and snapshot[1] and len(snapshot[2]) == len(PATIENTS)

    second = run_dag(1)  # replay: the file is found in processed/
    assert second["validate_lab_file"]["place"] == "processed"
    assert [query(pg_clean, sql) for sql in checks] == snapshot

    runs = query(pg_clean, "SELECT stage, status FROM pipeline_run_log")
    assert ("archive_file", "success") in runs and ("batch_vitals_job", "skipped") in runs
    assert all(status != "failed" for _, status in runs)


def test_corrupt_file_loads_good_rows_and_quarantines_bad_ones(env, pg_clean):
    rows, _ = corrupt_rows(generate_lab_rows(2, PATIENTS, CLOCK, 42), random.Random(3))
    drop(env, 2, rows)
    out = run_dag(2)
    v = out["validate_lab_file"]
    assert v["status"] == "ok" and v["rows_in"] == len(rows)
    assert v["rejected"]["unknown_patient"] == 1 and sum(v["rejected"].values()) == 5
    loaded = query(pg_clean, "SELECT count(*) FROM lab_results WHERE sim_day = 2")[0][0]
    assert loaded == v["rows_valid"] == len(rows) - 5
    rejects = (env / "landing" / "quarantine" / "labs_day_002.rejected.csv").read_text()
    assert "P999" in rejects and "unknown_patient" in rejects
    assert query(pg_clean, "SELECT count(*) FROM lab_results WHERE patient_id = 'P999'")[0][0] == 0


def test_bad_schema_file_is_quarantined_and_previous_labs_stay(env, pg_clean):
    drop(env, 1)
    run_dag(1)
    before = dict(query(pg_clean, "SELECT patient_id, lab_risk_points FROM patient_lab_risk"))

    columns = ("patient_id", "result_value", "reference_range", "collected_at")
    drop(env, 2, columns=columns)
    out = run_dag(2)
    assert out["validate_lab_file"]["status"] == "quarantined"
    assert out["load_lab_results"] == 0
    assert (env / "landing" / "quarantine" / "labs_day_002.csv").exists()
    assert (env / "landing" / "quarantine" / "labs_day_002.csv.reason.txt").exists()
    assert query(pg_clean, "SELECT count(*) FROM lab_results WHERE sim_day = 2")[0][0] == 0
    report = dict(
        query(
            pg_clean,
            "SELECT patient_id, lab_points_after FROM patient_risk_report WHERE sim_day = 2",
        )
    )
    assert report == {pid: before.get(pid, 0) for pid in report}  # previous labs in force
    assert out["data_quality_and_health_check"]["soft"]["lab_file"] == "quarantined"


def test_missing_file_callback_counts_and_logs(dirs, capsys):
    env = dirs
    ti = FakeTI("wait_for_lab_file", {"resolve_sim_day": 4})
    tasks.on_task_failure({"ti": ti, "run_id": "r", "exception": TimeoutError("sensor")})
    tasks.on_task_failure({"ti": ti, "run_id": "r", "exception": TimeoutError("sensor")})
    state = json.loads((env / "state" / "batch_metrics.json").read_text())
    assert state["wait_for_lab_file"] == {
        "airflow_task_failures_total": 2.0,
        "lab_file_missing_total": 2.0,
    }
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line]
    assert {"task_failed", "lab_file_missing"} <= {e["event"] for e in events}
    assert all(e["service"] == "airflow-batch" and e["stage"] for e in events)


def test_report_answers_the_business_question_for_occult_patients(env, pg_clean):
    """Normal vitals, abnormal labs: tier after labs is above tier before (speed-layer vitals)."""
    import psycopg2

    occult = [p.patient_id for p in PATIENTS if p.occult_from_day is not None]
    with psycopg2.connect(**pg_clean) as conn, conn.cursor() as cur:
        for pid in occult:  # speed layer: normal vitals, NEWS 0
            cur.execute(
                "INSERT INTO patient_status (patient_id, news_score, max_vital_score, risk_tier) "
                "VALUES (%s, 0, 0, 'LOW')",
                (pid,),
            )
    conn.close()
    day = max(p.occult_from_day for p in PATIENTS if p.occult_from_day) + 2
    drop(env, day)
    run_dag(day)
    rows = query(
        pg_clean,
        "SELECT patient_id, vitals_source, risk_before_labs, risk_after_labs, lab_points_after "
        "FROM patient_risk_report WHERE sim_day = %s AND patient_id = ANY(%s)",
        day,
        occult,
    )
    assert len(rows) == len(occult)
    for pid, source, before, after, points in rows:
        assert source == "speed" and before == "LOW", pid  # first labs: 0 points before
        assert points >= 2, pid
        if points >= 3:
            assert after != "LOW", pid
