"""Load the synthetic patient cohort into Postgres (table ``patients``).

The cohort is deterministic (``build_patients``), so re-running is a harmless
upsert. The scripted health story (who deteriorates, occult lab risk) is *not*
written to the database - it is hidden ground truth, saved as JSON by the vitals
simulator for later validation of alerts.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from common.config import Settings
from common.logging_setup import configure_logging, get_logger
from simulators.patients import PatientProfile, build_patients

log = get_logger("seed_patients", "storage")

UPSERT = """
INSERT INTO patients (patient_id, name, age, sex, ward, bed, comorbidity, comorbidity_flag,
                      baseline_hr, baseline_spo2, baseline_sbp, baseline_dbp, baseline_temp)
VALUES (%(patient_id)s, %(name)s, %(age)s, %(sex)s, %(ward)s, %(bed)s, %(comorbidity)s,
        %(comorbidity_flag)s, %(baseline_hr)s, %(baseline_spo2)s, %(baseline_sbp)s,
        %(baseline_dbp)s, %(baseline_temp)s)
ON CONFLICT (patient_id) DO UPDATE SET
    name = EXCLUDED.name, age = EXCLUDED.age, sex = EXCLUDED.sex, ward = EXCLUDED.ward,
    bed = EXCLUDED.bed, comorbidity = EXCLUDED.comorbidity,
    comorbidity_flag = EXCLUDED.comorbidity_flag, baseline_hr = EXCLUDED.baseline_hr,
    baseline_spo2 = EXCLUDED.baseline_spo2, baseline_sbp = EXCLUDED.baseline_sbp,
    baseline_dbp = EXCLUDED.baseline_dbp, baseline_temp = EXCLUDED.baseline_temp
"""

_DDL_CANDIDATES = (
    Path("/app/sql/01_patients.sql"),
    Path(__file__).resolve().parents[1] / "sql" / "01_patients.sql",
)


def patient_row(patient: PatientProfile) -> dict:
    return {
        "patient_id": patient.patient_id,
        "name": patient.name,
        "age": patient.age,
        "sex": patient.sex,
        "ward": patient.ward,
        "bed": patient.bed,
        "comorbidity": patient.comorbidity,
        "comorbidity_flag": patient.comorbidity_flag,
        "baseline_hr": patient.baseline_hr,
        "baseline_spo2": patient.baseline_spo2,
        "baseline_sbp": patient.baseline_sbp,
        "baseline_dbp": patient.baseline_dbp,
        "baseline_temp": patient.baseline_temp,
    }


def _connect(settings: Settings, timeout_s: float = 90.0):
    import psycopg2

    deadline = time.monotonic() + timeout_s
    delay = 1.0
    while True:
        try:
            return psycopg2.connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                user=settings.postgres_user,
                password=settings.postgres_password,
                dbname=settings.postgres_db,
                connect_timeout=5,
            )
        except psycopg2.OperationalError as exc:
            if time.monotonic() >= deadline:
                raise
            log.warning("postgres_unavailable", error=str(exc).strip(), retry_in_s=delay)
            time.sleep(delay)
            delay = min(delay * 2, 10.0)


def main() -> int:
    settings = Settings.from_env()
    configure_logging("seed-patients", settings.log_level)
    patients = build_patients(
        settings.num_patients,
        settings.sim_seed,
        settings.sim_day_seconds,
        settings.num_deteriorating,
        settings.num_occult,
    )
    ddl_path = next(p for p in _DDL_CANDIDATES if p.exists())

    conn = _connect(settings)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(ddl_path.read_text())
            cur.executemany(UPSERT, [patient_row(p) for p in patients])
    finally:
        conn.close()
    log.info("patients_seeded", rows=len(patients), database=settings.postgres_db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
