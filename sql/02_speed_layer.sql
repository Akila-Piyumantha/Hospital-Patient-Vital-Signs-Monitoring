-- Speed-layer tables (contract 4.4), written by the Spark streaming job (Member B).
-- Schema owner is Member C: these definitions are B's proposal for init.sql - C may move them
-- there unchanged (see docs/CHANGELOG.md). Idempotent: runs at Postgres first start AND at every
-- start of the streaming job (streaming/sinks.py: ensure_schema), so an existing volume is upgraded.

-- One row per patient and sliding event-time window (2 min long, every 30 s).
CREATE TABLE IF NOT EXISTS vitals_window (
    patient_id          TEXT        NOT NULL,
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    sim_day             INTEGER,
    n_readings          INTEGER     NOT NULL,
    avg_heart_rate      DOUBLE PRECISION,
    min_heart_rate      INTEGER,
    max_heart_rate      INTEGER,
    avg_spo2            DOUBLE PRECISION,
    min_spo2            INTEGER,
    max_spo2            INTEGER,
    avg_systolic_bp     DOUBLE PRECISION,
    min_systolic_bp     INTEGER,
    max_systolic_bp     INTEGER,
    avg_diastolic_bp    DOUBLE PRECISION,
    min_diastolic_bp    INTEGER,
    max_diastolic_bp    INTEGER,
    avg_temperature     DOUBLE PRECISION,
    min_temperature     DOUBLE PRECISION,
    max_temperature     DOUBLE PRECISION,
    news_score          INTEGER,            -- NEWS of the window averages (drives "sustained abnormal")
    trend_slope_hr      DOUBLE PRECISION,   -- per minute, regression over the last 5 windows
    trend_slope_spo2    DOUBLE PRECISION,
    trend_slope_sbp     DOUBLE PRECISION,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, window_start)
);
CREATE INDEX IF NOT EXISTS vitals_window_end_idx ON vitals_window (window_end);
CREATE INDEX IF NOT EXISTS vitals_window_day_idx ON vitals_window (sim_day);

-- Current state per patient (one row each), the ward's "right now" view.
CREATE TABLE IF NOT EXISTS patient_status (
    patient_id                 TEXT PRIMARY KEY,
    last_reading_at            TIMESTAMPTZ,
    sim_day                    INTEGER,
    heart_rate                 INTEGER,
    spo2                       INTEGER,
    systolic_bp                INTEGER,
    diastolic_bp               INTEGER,
    temperature                DOUBLE PRECISION,
    map                        DOUBLE PRECISION,
    pulse_pressure             INTEGER,
    news_score                 INTEGER,
    max_vital_score            INTEGER,
    lab_risk_points            INTEGER     NOT NULL DEFAULT 0,
    lab_as_of_sim_day          INTEGER,
    total_score                INTEGER,
    risk_tier                  TEXT CHECK (risk_tier IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    trend_flag                 TEXT        NOT NULL DEFAULT 'STABLE',
    sustained_abnormal_windows INTEGER     NOT NULL DEFAULT 0,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Patient alerts with an open -> resolved lifecycle; at most one open alert per patient+reason.
CREATE TABLE IF NOT EXISTS alerts (
    alert_id      UUID PRIMARY KEY,            -- uuid5(patient|reason|opened_at): replay-safe
    patient_id    TEXT        NOT NULL,
    severity      TEXT        NOT NULL CHECK (severity IN ('MEDIUM', 'HIGH', 'CRITICAL')),
    reason_code   TEXT        NOT NULL,
    value         DOUBLE PRECISION,
    threshold     DOUBLE PRECISION,
    opened_at     TIMESTAMPTZ NOT NULL,        -- event time of the triggering reading/window
    last_seen_at  TIMESTAMPTZ NOT NULL,
    resolved_at   TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS alerts_one_open_idx
    ON alerts (patient_id, reason_code) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS alerts_opened_idx ON alerts (opened_at);

-- Records rejected by validation (also published to Kafka topic vitals.dlq).
CREATE TABLE IF NOT EXISTS dlq_events (
    id              BIGSERIAL PRIMARY KEY,
    reason          TEXT        NOT NULL,
    payload         TEXT,
    failed_at       TIMESTAMPTZ NOT NULL,
    patient_id      TEXT,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    UNIQUE (kafka_partition, kafka_offset)     -- replaying a micro-batch cannot duplicate rows
);
CREATE INDEX IF NOT EXISTS dlq_events_failed_idx ON dlq_events (failed_at);

-- Owned and written by Member C (Airflow compute_lab_risk); the speed layer only READS it.
-- Defined here too so the streaming job can start before init.sql exists - C: keep this
-- definition identical in init.sql (or change both). One row per patient and lab day.
CREATE TABLE IF NOT EXISTS patient_lab_risk (
    patient_id      TEXT    NOT NULL,
    lab_risk_points INTEGER NOT NULL,
    abnormal_tests  TEXT[]  NOT NULL DEFAULT '{}',
    as_of_sim_day   INTEGER NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, as_of_sim_day)
);
