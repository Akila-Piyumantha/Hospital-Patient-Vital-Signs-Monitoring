-- Batch layer, serving and bookkeeping tables (contract 4.4). Owner: Member C.
--
-- Postgres runs docker-entrypoint-initdb.d files in alphabetical order:
--   00_create_databases.sql (A)  airflow metadata DB
--   01_patients.sql         (A)  patients (seeded by simulators/seed_patients.py)
--   02_speed_layer.sql      (B)  vitals_window, patient_status, alerts, dlq_events, patient_lab_risk
--   init.sql                (C)  this file
-- C has reviewed B's 02_speed_layer.sql and keeps it as a separate file (see docs/CHANGELOG.md), so the
-- streaming job can keep running its own idempotent DDL at start-up.
--
-- Idempotent (IF NOT EXISTS everywhere): the Airflow tasks run this file before writing, so a
-- Postgres volume created before this file existed is upgraded without a reset.
--
-- Day numbering used by every batch table: the lab file labs_day_N.csv arrives at the start of sim
-- day N and holds the labs collected on day N-1. Everything derived from it is keyed by the *file
-- day* N (lab_results.sim_day, patient_lab_risk.as_of_sim_day, patient_risk_report.sim_day); the
-- vitals the batch job recomputes for that report are those of day N-1 (vitals_sim_day).

-- Identical to the definition in 02_speed_layer.sql (the speed layer reads it; C writes it).
CREATE TABLE IF NOT EXISTS patient_lab_risk (
    patient_id      TEXT    NOT NULL,
    lab_risk_points INTEGER NOT NULL,
    abnormal_tests  TEXT[]  NOT NULL DEFAULT '{}',
    as_of_sim_day   INTEGER NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, as_of_sim_day)
);

-- Validated rows of the daily lab file. One lab day is replaced as a whole (delete + insert in one
-- transaction), so re-running a day can never duplicate results.
CREATE TABLE IF NOT EXISTS lab_results (
    sim_day        INTEGER          NOT NULL,   -- file day N (labs collected on day N-1)
    patient_id     TEXT             NOT NULL,
    test_type      TEXT             NOT NULL,
    result_value   DOUBLE PRECISION NOT NULL,
    ref_low        DOUBLE PRECISION NOT NULL,
    ref_high       DOUBLE PRECISION NOT NULL,
    abnormal_flag  TEXT CHECK (abnormal_flag IN ('HIGH', 'LOW')),   -- NULL = within range
    collected_at   TIMESTAMPTZ      NOT NULL,
    source_file    TEXT             NOT NULL,
    loaded_at      TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_day, patient_id, test_type)
);
CREATE INDEX IF NOT EXISTS lab_results_patient_idx ON lab_results (patient_id, sim_day);

-- Batch recompute of one day of vitals from the Parquet lake (task C4), one row per patient.
CREATE TABLE IF NOT EXISTS batch_vitals_daily (
    sim_day              INTEGER NOT NULL,      -- the vitals day (lake partition sim_day=N)
    patient_id           TEXT    NOT NULL,
    n_readings           INTEGER NOT NULL,
    first_reading_at     TIMESTAMPTZ,
    last_reading_at      TIMESTAMPTZ,
    avg_heart_rate       DOUBLE PRECISION,
    min_heart_rate       INTEGER,
    max_heart_rate       INTEGER,
    avg_spo2             DOUBLE PRECISION,
    min_spo2             INTEGER,
    max_spo2             INTEGER,
    avg_systolic_bp      DOUBLE PRECISION,
    min_systolic_bp      INTEGER,
    max_systolic_bp      INTEGER,
    avg_diastolic_bp     DOUBLE PRECISION,
    min_diastolic_bp     INTEGER,
    max_diastolic_bp     INTEGER,
    avg_temperature      DOUBLE PRECISION,
    min_temperature      DOUBLE PRECISION,
    max_temperature      DOUBLE PRECISION,
    pct_abnormal_hr      DOUBLE PRECISION,      -- share of readings whose sub-score > 0
    pct_abnormal_spo2    DOUBLE PRECISION,
    pct_abnormal_sbp     DOUBLE PRECISION,
    pct_abnormal_temp    DOUBLE PRECISION,
    pct_news_ge3         DOUBLE PRECISION,      -- share of readings with news_score >= 3
    peak_news_score      INTEGER,
    end_news_score       INTEGER,               -- NEWS of the averages over the last part of the day
    end_max_vital_score  INTEGER,
    hr_change            DOUBLE PRECISION,      -- avg of the last part minus avg of the first part
    spo2_change          DOUBLE PRECISION,
    sbp_change           DOUBLE PRECISION,
    trend                TEXT CHECK (trend IN ('WORSENING', 'IMPROVING', 'STABLE')),
    hr_series            DOUBLE PRECISION[],    -- bucketed averages for the report sparklines
    spo2_series          DOUBLE PRECISION[],
    sbp_series           DOUBLE PRECISION[],
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_day, patient_id)
);

-- Lambda reconciliation (task C4): batch recompute vs. the speed layer's vitals_window, per day.
CREATE TABLE IF NOT EXISTS speed_batch_reconciliation (
    sim_day              INTEGER PRIMARY KEY,   -- the vitals day
    windows_compared     INTEGER NOT NULL,
    windows_missing      INTEGER NOT NULL,      -- in the batch recompute, absent from vitals_window
    batch_readings       BIGINT  NOT NULL,      -- sum of n_readings over the compared windows
    speed_readings       BIGINT  NOT NULL,
    count_abs_diff       BIGINT  NOT NULL,
    mean_abs_diff_hr     DOUBLE PRECISION,      -- |avg HR batch - avg HR speed|, mean over windows
    discrepancy_ratio    DOUBLE PRECISION NOT NULL,   -- count_abs_diff / batch_readings
    lake_readings        BIGINT  NOT NULL,      -- distinct readings of the day in the lake
    lake_duplicates      BIGINT  NOT NULL,      -- rows removed by dropDuplicates(event_id)
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Daily consolidated risk report (task C5): how yesterday's labs change each patient's risk.
CREATE TABLE IF NOT EXISTS patient_risk_report (
    sim_day            INTEGER NOT NULL,        -- file day N the report is produced for
    patient_id         TEXT    NOT NULL,
    rank               INTEGER NOT NULL,        -- 1 = highest risk after labs
    vitals_sim_day     INTEGER NOT NULL,        -- N-1
    vitals_source      TEXT    NOT NULL CHECK (vitals_source IN ('batch', 'speed', 'none')),
    vitals_summary     TEXT    NOT NULL,
    lab_summary        TEXT    NOT NULL,
    news_score         INTEGER NOT NULL,
    max_vital_score    INTEGER NOT NULL,
    trend              TEXT,
    lab_points_before  INTEGER NOT NULL,        -- lab points in force during day N-1
    lab_points_after   INTEGER NOT NULL,        -- after this file (used by the speed layer on day N)
    total_before       INTEGER NOT NULL,
    total_after        INTEGER NOT NULL,
    risk_before_labs   TEXT    NOT NULL CHECK (risk_before_labs IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    risk_after_labs    TEXT    NOT NULL CHECK (risk_after_labs IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    tier_change        TEXT    NOT NULL CHECK (tier_change IN ('UP', 'DOWN', 'SAME')),
    abnormal_tests     TEXT[]  NOT NULL DEFAULT '{}',
    alerts_opened      INTEGER NOT NULL DEFAULT 0,   -- speed-layer alerts opened on day N-1
    generated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_day, patient_id)
);
CREATE INDEX IF NOT EXISTS patient_risk_report_rank_idx ON patient_risk_report (sim_day, rank);

-- One row per pipeline step execution (all layers may write it; the DAG writes one per task).
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id           BIGSERIAL PRIMARY KEY,
    stage        TEXT        NOT NULL,          -- e.g. validate_lab_file, batch_vitals_job
    run_id       TEXT        NOT NULL,          -- Airflow run_id (or any trace id)
    sim_day      INTEGER,
    status       TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed', 'skipped')),
    rows_in      BIGINT,
    rows_out     BIGINT,
    details      JSONB       NOT NULL DEFAULT '{}',
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS pipeline_run_log_stage_idx ON pipeline_run_log (stage, started_at DESC);
CREATE INDEX IF NOT EXISTS pipeline_run_log_day_idx ON pipeline_run_log (sim_day);
