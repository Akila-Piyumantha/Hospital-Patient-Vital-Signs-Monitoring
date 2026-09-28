# Batch layer, serving and reporting (Member C)

Design notes, failure handling (C7) and report chapter drafts (C9) for the Airflow DAG, the batch
Spark job, the daily risk report and the FastAPI serving layer. Code: [batch/](../batch),
[airflow/dags/](../airflow/dags), [serving/api/](../serving/api), schema [sql/init.sql](../sql/init.sql).

## 1. How a simulated day flows through the batch layer

```
sim day N starts ─► lab-generator drops labs_day_N.csv (labs collected on day N-1)
                     │
  Airflow run (every SIM_DAY_SECONDS, target day = current sim day or conf {"sim_day": N})
   resolve_sim_day ─► wait_for_lab_file (FileSensor, reschedule, 80 % of a day)
   ─► validate_lab_file   header, types, reference ranges, timestamps, unknown patients, duplicates
                          bad rows  -> quarantine/labs_day_N.rejected.csv (+ reason column)
                          bad file  -> quarantine/labs_day_N.csv + .reason.txt, nothing loaded
   ─► load_lab_results    lab_results: day N replaced as a whole (delete + insert, one transaction)
   ─► compute_lab_risk    patient_lab_risk (as_of_sim_day = N) with common/scoring.py lab points
                          -> the speed layer picks it up in its next micro-batch (B8)
   ─► batch_vitals_job    Spark: lake sim_day=N-1 -> batch_vitals_daily + reconciliation
   ─► build_risk_report   patient_risk_report + data/reports/risk_report_day_NNN.{html,csv}
   ─► data_quality_and_health_check   hard checks fail the run, soft checks warn
   ─► archive_file        labs_day_N.csv -> processed/, push airflow_dag_last_success_timestamp
```

Every task writes a `pipeline_run_log` row (`running` → `success`/`failed`/`skipped`, rows in/out,
details as JSON) and logs JSON events (contract 4.8, `service="airflow-batch"`). Failures call
`on_task_failure` (JSON event + `airflow_task_failures_total`); a sensor timeout also increments
`lab_file_missing_total`, which fires `LabFileMissing`.

**Why the batch job waits.** Day N-1 is recomputed only after `day_end(N-1) + BATCH_SETTLE_SECONDS`
(75 s = 1-min watermark + one trigger + margin), so the speed layer has had the chance to close its
last windows and write the last lake files. Without the wait, the reconciliation would blame the speed
layer for readings that simply had not arrived yet.

### 1.1 Speed-vs-batch reconciliation (the Lambda evidence)

The batch job rebuilds the speed layer's own windows (2 min sliding 30 s, `F.window` on event time,
aligned like Spark Structured Streaming) from the deduplicated lake, keeps only windows lying
completely inside the day, and compares per `(patient, window_start)`:

```
discrepancy_ratio = Σ |n_batch − n_speed| / Σ n_batch        (+ mean |avg HR batch − avg HR speed|)
```

A window the speed layer never wrote counts with `n_speed = 0`. Sources of a non-zero ratio: readings
later than the 1-min watermark (kept in the lake, dropped from windows), speed-layer downtime, and any
logic drift between the layers. The ratio lands in `speed_batch_reconciliation`, the report header,
`/api/ward/summary` and Prometheus (`speed_batch_discrepancy_ratio`, alert `SpeedBatchDiscrepancy`
above 10 %).

### 1.2 Risk before vs. after labs

For report day N and each patient: `NEWS_ref` = NEWS of the vital averages over the last quarter of
day N-1 (batch), or the speed layer's latest NEWS when the lake had no data (`vitals_source`).

| | lab points | tier |
|---|---|---|
| `risk_before_labs` | newest `patient_lab_risk` with `as_of_sim_day < N` (what the ward saw on day N-1) | `risk_tier(NEWS_ref + before, max_vital)` |
| `risk_after_labs` | newest with `as_of_sim_day <= N` (what the speed layer applies from day N on) | `risk_tier(NEWS_ref + after, max_vital)` |

Ranked by tier after labs, then total, then NEWS. Occult patients (normal vitals, abnormal labs) are
exactly the rows with `tier_change = UP` and a low NEWS.

## 2. Failure handling and idempotency (C7)

| Case | How to trigger | Expected / designed outcome | Verified by |
|---|---|---|---|
| **Replay a sim day** | `make replay-day DAY=3` (`airflow dags trigger daily_lab_risk_report -c '{"sim_day": 3}'`) | Sensor finds the file in `processed/`; `lab_results`, `patient_lab_risk`, `batch_vitals_daily`, `speed_batch_reconciliation`, `patient_risk_report` and the HTML/CSV are replaced, not duplicated - identical rows after the replay | `tests/batch/test_dag_tasks_db.py::test_full_run_then_replay_is_idempotent` |
| **Corrupt rows** | `LAB_FORCE_CORRUPT_DAYS=5` | The 5 injected defects (non-numeric value, unknown patient, bad range, empty timestamp, duplicate) go to `quarantine/labs_day_005.rejected.csv` with their reason; all other rows load; `lab_rows_quarantined_total` +5; run succeeds | `test_corrupt_file_loads_good_rows_and_quarantines_bad_ones`, `tests/batch/test_lab_pipeline.py` |
| **Unknown patient** | part of the corrupt file (`P999`) | Row rejected as `unknown_patient`, never reaches `lab_results` / `patient_lab_risk` | same tests |
| **Bad schema** (missing column) | `LAB_FORCE_BADSCHEMA_DAYS=6` | Whole file → `quarantine/` + `.reason.txt`; load and lab risk skipped (previous lab risk stays in force, the speed layer is unaffected); report still built with the previous labs; health check warns; `lab_files_quarantined_total` +1 | `test_bad_schema_file_is_quarantined_and_previous_labs_stay` |
| **Missing file** | `LAB_FORCE_MISSING_DAYS=4` | Sensor times out after 80 % of a day → task failed → `lab_file_missing_total` +1 → `LabFileMissing` fires; downstream tasks `upstream_failed`; next day's run is independent | `test_missing_file_callback_counts_and_logs`, alert rule unit test (A) |
| **Late file** | `LAB_FORCE_LATE_DAYS=3` (90 s late) | Sensor keeps rescheduling (poke every 10 s) and proceeds when the file lands; batch job wait unaffected | design (sensor timeout 240 s > 90 s) |
| **Speed layer down during a day** | `docker compose stop spark-streaming` for a few minutes | Spark resumes from its checkpoint and re-reads the missed Kafka offsets, so the lake and the windows both catch up and the discrepancy stays low; if the day is recomputed before Spark is back, the missing windows show as `windows_missing` and the ratio rises (a replay after recovery corrects it) | reconciliation `windows_missing` |
| **Transient DB / Pushgateway error** | stop `pushgateway` | Metric pushes are best effort (logged `metrics_push_failed`), tasks succeed; DB errors are retried twice (10 s) | `tests/batch/test_metrics.py::test_push_failure_is_not_fatal` |
| **Task crash mid-way** | kill the scheduler during `load_lab_results` | Every write is one transaction; the retry replaces the day; `pipeline_run_log` shows the `running` row of the killed attempt and the `success` of the retry | design |

### 2.1 Live run on the full stack (2026-09-28, Docker Desktop, 8 GB)

`docker compose up -d --build` with every service (A + B + C). Observed:

* DAG parsed with no import errors; scheduled runs for days 3 and 4 and manual replays of days 1 and 2
  (`airflow dags trigger ... -c '{"sim_day": N}'`) all ended `success`; every task wrote its
  `pipeline_run_log` row; lab files moved to `processed/`; `risk_report_day_00{1..4}.{html,csv}` written.
* Batch Spark job (local mode inside the scheduler): ~67 s per day including start-up.
* Reconciliation, days 1-3: discrepancy 0.91 % / 0.56 % / 0.19 % of readings (speed layer counted fewer:
  late readings dropped by the 1-min watermark), 0 missing windows, mean |ΔHR| ≤ 0.02 bpm, 0 lake
  duplicates. Day 1 compared 80 windows instead of 120 (stack started mid-day).
* Lab feedback: `patient_status.lab_as_of_sim_day = 4` for all patients seconds after the day-4 run;
  report day 4: 5 tier changes, e.g. P012 NEWS 0, lab points 1 → 4 (creatinine, CRP, lactate, WBC
  high): LOW → MEDIUM.
* Prometheus: `airflow_dag_last_success_timestamp`, `speed_batch_discrepancy_ratio` (0.0019),
  `lab_files_ingested_total` (4), `lab_file_missing_total` (0), API `up`, `ward_patients` and
  `pipeline_last_event_age_seconds` (Spark + API); no alert firing.
* API: `/health` 200, `/api/ward/summary`, `/api/reports/risk/latest` (+ `/html`, 30 kB),
  `/api/patients/P012`, `/api/pipeline/runs` answered with live data.

* Missing file, live: `LAB_FORCE_MISSING_DAYS=7` → lab-generator logged `lab_file_not_dropped`
  (day 7) → the day-7 run's sensor timed out after 240 s → `lab_file_missing_total` 0 → 1 →
  `LabFileMissing` firing in Prometheus → Alertmanager → `data/alerts/alerts.jsonl`
  (`alert_firing`). Days 1-6 ran green around it.
* Memory (steady state): spark-streaming 1.6 GiB, airflow-webserver 0.8 GiB, airflow-scheduler 0.5 GiB
  (peaks during the batch Spark job), kafka 0.4 GiB.

Bugs found by the live run and fixed: (1) the first scheduled run after start-up resolved **day 0**
(its data interval ended before the simulators created the sim epoch) - days are now clamped to ≥ 1;
(2) `archive_file` failed with `Permission denied` - `data/landing` and `data/state` were created by the
root-run simulators while Airflow runs as `AIRFLOW_UID`; `airflow-init` now creates and chowns them
(official Airflow Compose pattern); (3) the three Airflow services shared one image tag and failed to
build in parallel - the tag was removed.

## 3. Serving layer

FastAPI app `serving/api/main.py`, OpenAPI docs at http://localhost:8000/docs.

| Endpoint | Source tables | Notes |
|---|---|---|
| `GET /api/ward/summary` | `patient_status`, `alerts`, `vitals_window` (speed) + `patient_risk_report`, `patient_lab_risk`, `speed_batch_reconciliation`, `pipeline_run_log` (batch) | tier counts, alerts by severity, average vitals, readings/min, freshness, batch status, patients that need attention now |
| `GET /api/patients`, `/{id}` | `patients` ⋈ `patient_status` ⋈ newest `patient_lab_risk` | sorted by tier/total; filters `risk_tier`, `ward`; `limit`/`offset` |
| `GET /api/patients/{id}/vitals?minutes=10` | `vitals_window` | anchored at the patient's newest window |
| `GET /api/alerts?status=open&severity=` | `alerts` | `status` open/resolved/all, `patient_id`, `since`, pagination |
| `GET /api/reports/risk[/latest,/{sim_day},/{sim_day}/html]` | `patient_risk_report`, reconciliation, `data/reports` | JSON report + rendered HTML |
| `GET /api/pipeline/runs` | `pipeline_run_log` | batch run history |
| `GET /health` | `patient_status` | 503 when the DB is down or the newest reading is older than 60 s |
| `GET /metrics` | - | `api_request_duration_seconds`, `pipeline_last_event_age_seconds`, `ward_patients`, `ward_open_alerts` |

Every request is logged as JSON (`stage="serving"`, route, status, duration). Postgres access is a
small thread-safe pool of read-only transactions with a 5 s statement timeout; a DB outage turns into
`503 {"detail": "database unavailable"}` instead of a stack trace.

## 4. Report chapter drafts (C9)

### 4.1 Use case and interpreted requirements

The ward needs to know *which patients show concerning vital-sign trends right now* and *how
yesterday's lab results change the risk picture going forward*. We read this as five requirements:

1. **Seconds-level awareness** - per-patient risk and alerts from continuous bedside vitals
   (HR, SpO₂, BP, temperature) within one trigger interval (5 s) of a reading.
2. **Trends, not just thresholds** - deterioration shows as rising HR / falling SpO₂ and SBP over
   minutes, so windowed aggregation and slopes are required, not only per-reading checks.
3. **Labs arrive once a day and can be late, missing, corrupt or corrected** - they need validation,
   quarantine and idempotent reloads, not a streaming path.
4. **Labs must feed back into the real-time view** - a patient with normal vitals but a high lactate is
   at risk; the lab points must raise the live tier (lab → speed-layer feedback loop).
5. **A trustworthy daily consolidated report** - recomputed from immutable data, ranked, explaining
   the change caused by the labs, and reproducible for any past day.

Non-functional: runs on one laptop with Docker Compose; observable (logs, metrics, alerts); every
component replay-safe. Simplifications: 20 synthetic patients, 1 sim day = 5 min, simplified NEWS2
(not clinically validated).

### 4.2 Serving layer

The serving layer is where the Lambda views meet. The speed layer continuously upserts small,
query-ready tables (`patient_status`, `vitals_window`, `alerts`); the batch layer writes daily tables
(`patient_lab_risk`, `patient_risk_report`, `batch_vitals_daily`, `speed_batch_reconciliation`). One
PostgreSQL instance hosts both, which keeps the merge a SQL join instead of a separate merge service -
appropriate at 20 patients (a few thousand rows a day) and it gives Grafana and the API the same
source. The API is read-only, paginated, documented by OpenAPI and exposes latency histograms and
freshness gauges; `/health` encodes the operational rule "data older than 60 s is an incident".

### 4.3 Consistency paragraph for the Lambda chapter (serving / merging the views)

The two layers are merged at two points. (1) **Lab risk into the speed view**: the batch layer writes
`patient_lab_risk` keyed by lab day; the streaming job re-reads the newest row per patient every
micro-batch, so the live tier includes yesterday's labs seconds after the DAG commits - an eventually
consistent merge with a bounded lag of one trigger. (2) **Daily report from the batch view**: the report
is computed from the immutable lake, not from the speed tables, so late or duplicated readings that the
watermark-bound speed layer dropped or double-processed are handled correctly. The price of Lambda is
two code paths; we contain it with one scoring module (`common/scoring.py`) used by both and we
*measure* the remaining disagreement every day (`speed_batch_discrepancy_ratio`), which turns the
consistency argument into a number we can show.

### 4.4 Results (to fill from the soak run)

Screenshots to capture: Airflow graph view of a green run and of a sensor timeout; `/docs`;
`/api/ward/summary`; a patient whose `risk_after_labs` is above `risk_before_labs` in the HTML
report; Grafana ward dashboard with the report panel; `speed_batch_discrepancy_ratio` over 6+ days.

### 4.5 Limitations and production-scale changes

* Report vitals score = NEWS of end-of-day averages: smooths short spikes (peak NEWS is shown next to
  it). Day-level trend thresholds (HR +10, SpO₂ −3, SBP −15) are illustrative.
* A scheduled run targets the sim day of its `data_interval_end` (exactly one day apart, so a late start
  cannot skip a day); with `catchup=False`, days missed while Airflow itself was down are not re-run
  automatically - `make replay-day DAY=N` does it. A production DAG would enable catch-up.
* Batch Spark runs in local mode inside the Airflow scheduler (LocalExecutor) - fine for a day of 3 000
  readings; at hospital scale it would be submitted to a cluster (SparkSubmitOperator / KubernetesPod),
  with the lake on S3 + Iceberg/Delta and partition overwrite instead of delete + insert.
* One Postgres for serving and Airflow metadata; production would separate them, add read replicas and
  put the API behind authentication, audit logging and role-based access (PHI).
