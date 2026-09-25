-- Static patient dimension (contract 4.4). Written by simulators/seed_patients.py.
-- Owner: Member A. Idempotent: safe to run at container start AND from the seeding script.
-- Member C: keep this table out of init.sql (or leave the definition identical).
CREATE TABLE IF NOT EXISTS patients (
    patient_id        TEXT PRIMARY KEY,
    name              TEXT        NOT NULL,
    age               INTEGER     NOT NULL,
    sex               CHAR(1)     NOT NULL,
    ward              TEXT        NOT NULL,
    bed               TEXT        NOT NULL,
    comorbidity       TEXT        NOT NULL DEFAULT 'none',
    comorbidity_flag  BOOLEAN     NOT NULL DEFAULT FALSE,
    baseline_hr       INTEGER     NOT NULL,
    baseline_spo2     INTEGER     NOT NULL,
    baseline_sbp      INTEGER     NOT NULL,
    baseline_dbp      INTEGER     NOT NULL,
    baseline_temp     NUMERIC(4,1) NOT NULL,
    admitted_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
