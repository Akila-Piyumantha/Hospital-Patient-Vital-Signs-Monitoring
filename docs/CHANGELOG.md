# Contract changelog

Any change to a shared contract (PROJECT_PLAN.md section 4: Kafka schema, lab file, Parquet layout,
Postgres tables, API, metric names, log format) gets an entry here. Changes need all three
members to agree. Status: **proposed** (needs the named member to confirm) -> **agreed**; **FYI** = owned by one member, no agreement needed.

| Date | Change | Proposed by | Status |
|---|---|---|---|
| 2026-09-25 | 4.4: pinned column names of `vitals_window` (`avg_/min_/max_` + vital name) and `patient_status` (latest vitals named after the Kafka fields; `risk_tier` values). Grafana ward dashboard reads them. | A | proposed - B to confirm |
| 2026-09-25 | 4.7: Pushgateway (`pushgateway:9091`) is the path for batch/Airflow metrics; Spark metrics served at `spark-streaming:8003/metrics`; API at `api:8000/metrics`. Metrics from Airflow carry label `dag_id` (`airflow_dag_last_success_timestamp{dag_id="daily_lab_risk_report"}`). | A | proposed - B/C to confirm |
| 2026-09-25 | 4.7: additional simulator metrics (`vitals_faults_injected_total{type}`, `simulator_*`, `lab_files_dropped_total`, `lab_files_skipped_total`, `lab_files_faulty_total`). | A | FYI - A-owned, no action needed |
| 2026-09-25 | 4.1: added `common/schemas.py` (`VitalReading`, `VITALS_SPARK_DDL`) as the code reference of the Kafka contract. | A | FYI - A-owned, available to B/C |
| 2026-09-25 | 4.2: `data/ground_truth/episodes.json` written by the simulator (hidden scripted story) for validating alerts and lab risk; **not** in the database. | A | FYI - A-owned, available to B/C |
| 2026-09-25 | Kafka consumer group: the Spark job should set a fixed `kafka.group.id` so `kafka_consumergroup_lag` (alert `ConsumerLagHigh`) is visible. | A | proposed - B to confirm |
