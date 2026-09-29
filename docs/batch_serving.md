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

Fault injections repeated live on 2026-09-29 (replay of day 3, corrupt file, bad schema, missing file),
with screenshots: see §4.3.

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

## 4. Report chapters (C9)

Final text for Member C's report sections. Figures refer to [docs/screenshots/](screenshots). The
consistency paragraph for the Lambda chapter lives in [speed_layer.md §1](speed_layer.md#1-architecture-decision-lambda-not-kappa)
next to B's and A's text; a copy is in §4.5.

### 4.1 Use case and interpreted requirements

A hospital ward monitors 20 patients with bedside devices that report heart rate, SpO₂, blood pressure
and temperature every few seconds. Once a day, the laboratory delivers a file with the previous day's
blood results. Clinicians ask two questions of different kinds: *which patients show concerning
vital-sign trends right now*, and *how do yesterday's lab results change the risk picture going
forward*. The first calls for seconds-level latency over a continuous stream. The second depends on a
file that arrives once a day and may be late, missing, corrupt or corrected. We interpreted the brief as
five functional requirements:

| # | Requirement | Where it is met |
|---|---|---|
| R1 | **Seconds-level awareness:** per-patient risk tier and alerts within one trigger interval (5 s) of a reading | Speed layer: Spark Structured Streaming → `patient_status`, `alerts` |
| R2 | **Trends, not only thresholds:** deterioration shows as rising HR, falling SpO₂ or falling SBP over minutes | 2-min sliding windows, slopes and a sustained-abnormal counter (`vitals_window`) |
| R3 | **Robust daily lab ingestion:** late, missing, corrupt or repeated files must not corrupt the data | Airflow DAG: sensor, validation, quarantine, idempotent day reload |
| R4 | **Labs feed back into the live view:** a patient with normal vitals but a high lactate is at risk | `patient_lab_risk`, re-read by the streaming job every micro-batch |
| R5 | **A trustworthy daily consolidated report:** recomputed from immutable data, ranked, explaining what the labs changed, reproducible for any past day | Batch Spark job over the Parquet lake → `patient_risk_report` + HTML/CSV |

Non-functional requirements: the system runs on one laptop with Docker Compose; it is observable
(structured logs, Prometheus metrics, alert rules, dashboards); and every component is safe to replay.
Simplifications, stated up front: synthetic patients from a seeded simulator; one simulated day = 5
real minutes, so clinical time windows are scaled down; and a simplified NEWS2 score without
respiration rate or consciousness level, which is not clinically validated.

### 4.2 Serving layer

The serving layer is where the two Lambda views meet. The speed layer continuously upserts small,
query-ready tables: `patient_status` (one row per patient), `vitals_window` and `alerts`. The batch layer
writes daily tables: `lab_results`, `patient_lab_risk`, `patient_risk_report`, `batch_vitals_daily` and
`speed_batch_reconciliation`. Both sets live in one PostgreSQL database, so merging them is a SQL join
rather than a separate merge service. At 20 patients (a few thousand rows a day) that is the simplest
correct choice, and it gives Grafana and the API the same source of truth.

The read API is a FastAPI application (`serving/api/`) with 13 read-only endpoints in five groups
(Fig. C-1, §3 lists them):

* **ward:** `/api/ward/summary` combines both layers in one response. It returns tier counts, open
  alerts by severity and average vitals from the speed tables, together with the latest report day,
  the latest lab day, the last DAG success and the last speed-vs-batch discrepancy from the batch
  tables (Fig. C-2).
* **patients:** a paginated list sorted by risk, plus a detail view. The detail joins the live status
  with the newest lab risk (Fig. C-3), so a clinician sees the vital-sign score and the lab points that
  make up the total.
* **alerts** and **pipeline runs:** filterable and paginated (`{items, total, limit, offset}`).
* **reports:** the daily risk report as JSON and as rendered HTML for any past day.
* **ops:** `/health` encodes the operational rule "data older than 60 s is an incident" and returns 503
  when it is broken. `/health/live` is the Docker liveness probe, and `/metrics` exposes the request
  latency histogram, data freshness, patients per tier and open alerts per severity.

Design choices: the API holds a small pool of read-only transactions with a 5 s statement timeout, so a
slow query cannot pin a connection. A database outage returns `503 database unavailable` instead of a
stack trace. Every request is logged as one JSON line with route, status and duration. The OpenAPI
document is generated from the pydantic response models, so documentation and code cannot drift apart.

### 4.3 Results

All figures come from the full stack (every service of Members A, B and C) on one laptop with Docker
Desktop. **Soak run:** 2026-09-28, 15:07–18:45 UTC, about 3 h 40 min or 43 simulated days, with no
manual intervention apart from the planned fault injections. **Fault runs:** 2026-09-29 on the same
data volumes.

**Batch layer throughput and reliability.** Every simulated day except one ended with a successful DAG run. The exception
was day 7, the planned missing-file day, which failed as designed (see C7 below). Days 1 and 2 first
failed in `archive_file` because of a folder-permission bug, which was then fixed; their replays
succeeded (§2.1). Every lab file that arrived was validated, loaded, scored, reported and archived
(42 files, 5 077 lab rows, 840 report rows). Task durations from
`pipeline_run_log`:

| Task | Runs | Mean | Max |
|---|---|---|---|
| `validate_lab_file`, `load_lab_results`, `compute_lab_risk`, `build_risk_report`, `archive_file` | 42–45 each | 0.2–0.3 s | 1.8 s |
| `batch_vitals_job` (Spark, local mode, including JVM start-up) | 42 | 85 s | 295 s |

The batch Spark job dominates. It stayed well inside the 5-minute simulated day, and its maximum
coincided with the speed layer's peak load (Fig. C-6).

**Speed vs batch consistency (the Lambda evidence).** For each of 41 vitals days the batch job rebuilt
the speed layer's 2-minute windows from the lake and compared them (Fig. C-7):

| Metric (41 days) | Value |
|---|---|
| Readings in the lake | 107 615 |
| Discrepancy ratio Σ\|n_batch − n_speed\| / Σn_batch | mean **0.54 %**, min 0.13 %, max 0.99 % |
| Windows the speed layer never wrote | **0** |
| Mean \|avg HR batch − avg HR speed\| per window | ≤ 0.034 bpm |
| Duplicate readings in the lake | 0 |

The speed layer always counted slightly fewer readings than the batch layer. These are readings that
arrived later than the 1-minute watermark: the lake kept them, but the streaming windows had already
closed. The numbers show the trade-off Lambda makes explicit. The real-time view gives up well under
1 % of completeness for seconds-level latency, and the batch view restores it. The ratio stayed an
order of magnitude below the 10 % alert threshold (`SpeedBatchDiscrepancy`) throughout.

**Labs change the risk picture.** Across 42 daily reports (840 patient-days), the labs moved the
risk tier of a patient 94 times: 50 up, 44 down. The report highlights these rows. Example (Fig. C-4,
day 42): P007 had an end-of-day NEWS of 0, so on vital signs alone they were LOW risk. The day-42 lab
file flagged creatinine and potassium high (0 → 3 lab points), which moved P007 to MEDIUM. This is
the "occult" patient the batch layer exists to catch. The same lab points reach the live view: after
each DAG run every `patient_status` row carries the new `lab_as_of_sim_day`, and the Grafana ward
panel shows lab points next to the live NEWS (Fig. C-5).

**Serving.** The API answered every endpoint with live data throughout. In Prometheus its p95 latency
was mostly below 50 ms, with one spike of about 450 ms while the batch Spark job and the streaming job
peaked at the same time (Fig. C-6, "API latency p95"). During the soak run the speed layer opened 905
alerts (45 critical, 755 high, 105 medium). All of them were later resolved; none was left open.

**Failure handling (C7).** Each case was run live on 2026-09-29:

| Case | Evidence | Outcome |
|---|---|---|
| Replay of day 3 | Fig. C-8, C-9 | `airflow dags trigger -c '{"sim_day": 3}'` → all 9 tasks green. All five derived tables were rewritten (new timestamps) with **identical row counts and content hashes**, so there were no duplicates. |
| Corrupt rows + unknown patient (day 176) | Fig. C-10 | 5 defective rows (bad reference range, `N/A` value, unknown patient `P999`, empty timestamp, duplicate) went to `quarantine/labs_day_176.rejected.csv` with a reason each. 117 of 122 rows loaded, the run succeeded, `lab_rows_quarantined_total` = 5, and `P999` does not appear in `lab_results` or `patient_lab_risk`. |
| Bad schema (day 177) | Fig. C-11 | The header lacks `test_type`, so the whole file went to `quarantine/` with `missing_columns:test_type` in its `.reason.txt`. Load and lab risk were `skipped`, and 0 rows were written. The day-176 lab risk stayed in force in both `patient_lab_risk` and the live `patient_status`. The report was still built, the health check warned (`lab_file: quarantined`), and `lab_files_quarantined_total` = 1. |
| Missing file (day 178) | Fig. C-12, C-13, C-14 | The lab generator logged `lab_file_not_dropped`. After 247 s the `wait_for_lab_file` sensor raised `AirflowSensorTimeout` (limit 240 s), and its failure callback raised `lab_file_missing_total` from 1 to 2 (day 7 of the soak run was the first). `LabFileMissing` was firing in Prometheus 13 s later and reached `data/alerts/alerts.jsonl` via Alertmanager 7 s after that. Downstream tasks were `upstream_failed`, and the next day's run was green. |

**Figures** (`docs/screenshots/`):

| Fig. | File | Shows |
|---|---|---|
| C-1 | `c9_api_docs.png` | OpenAPI documentation of the serving API |
| C-2 | `c9_api_ward_summary.png` | `/api/ward/summary`: speed and batch views in one response |
| C-3 | `c9_api_patient_P007.png` | Patient detail: live status + newest lab risk |
| C-4 | `c9_report_day042.png` | Daily consolidated risk report, day 42, with the P007 tier change highlighted |
| C-5 | `c9_grafana_ward_live.png` | Grafana ward live view (live tier, NEWS, lab points) |
| C-6 | `c9_grafana_pipeline_soak.png` | Pipeline-health dashboard over the soak run (times in IST, UTC+5:30) |
| C-7 | `c9_discrepancy_soak.png` | `speed_batch_discrepancy_ratio` per day over the soak run |
| C-8 | `c7_replay_airflow_graph.png` | Airflow graph of the day-3 replay run, all tasks successful |
| C-9 | `c7_replay_idempotency.png` | Day-3 tables before and after the replay |
| C-10 | `c7_corrupt_file_unknown_patient.png` | Row-level quarantine and the unknown patient |
| C-11 | `c7_bad_schema_file.png` | Whole-file quarantine of a file with a missing column |
| C-12 | `c7_missing_file_airflow.png` | Sensor timeout of the missing-file run in the Airflow grid |
| C-13 | `c7_missing_file_alert.png` | `LabFileMissing` firing in Prometheus |
| C-14 | `c7_missing_file_evidence.png` | End-to-end chain: generator log → metric → Prometheus → Alertmanager webhook |

### 4.4 Limitations and production-scale changes

* **Scoring is simplified.** The score is an adaptation of NEWS2 without respiration rate or
  consciousness level, and it is not clinically validated. The report's vitals score is the NEWS of
  the end-of-day averages, which smooths short spikes (peak NEWS is shown next to it). Day-level
  trend thresholds (HR +10, SpO₂ −3, SBP −15) are illustrative.
* **Time is compressed.** One simulated day is 5 minutes, so clinical windows are scaled down and
  the batch job has a 5-minute budget per day. At real time scales it would have a day.
* **Catch-up is manual.** A scheduled run targets the sim day of its `data_interval_end`, so a late
  start cannot skip a day. With `catchup=False`, however, days missed while Airflow itself was down are
  not re-run automatically. `make replay-day DAY=N` covers this, and a production DAG would enable
  catch-up.
* **Batch Spark runs in local mode** inside the Airflow scheduler (LocalExecutor). That is fine for
  about 6 000 readings a day, but in the soak run it took up to 5 minutes and competed with the
  streaming job for CPU. At hospital scale the job would be submitted to a cluster
  (SparkSubmitOperator or KubernetesPodOperator). The lake would move to object storage with a table
  format (Iceberg or Delta), and a partition overwrite would replace the delete + insert.
* **One Postgres** serves both views and holds the Airflow metadata. Production would separate them
  and add read replicas for the API.
* **No security layer.** The API has no authentication. Patient data (PHI) would require
  authentication, role-based access, audit logging and TLS, and the report files would move from a
  shared folder to access-controlled storage.
* **Reconciliation covers counts and heart rate only.** It compares window counts and mean HR. A
  production version would compare every vital sign and the NEWS per window, and alert on drift in
  the scoring logic separately from late data.

### 4.5 Consistency paragraph (Lambda chapter)

**Serving and consistency: how the two views are merged (C).** The speed and batch views meet in one
PostgreSQL database, at two points with different consistency guarantees. (1) **Lab risk into the live
view:** the batch layer writes `patient_lab_risk` keyed by lab day. The streaming job re-reads the newest
row per patient in every micro-batch, so the live tier in `patient_status` includes yesterday's labs within
one trigger (about 5 s) of the DAG committing. This merge is eventually consistent with a bounded lag,
and because a quarantined lab file writes nothing, the previous lab risk stays in force rather than
dropping to zero. (2) **The daily report from the batch view:** `patient_risk_report` is computed from the
immutable Parquet lake, not from the speed tables. Readings that arrived after the 1-minute watermark,
which the speed layer dropped from its windows, and duplicates that the lake deduplicates by `event_id`
are therefore counted correctly, and replaying a day reproduces the same report. The API
(`/api/ward/summary`, `/api/patients/{id}`) then joins the live tables with the newest batch rows at
query time and exposes the timestamp of each side (data age, latest report day, latest lab day), so a
reader can see how fresh each part of the answer is. The cost of Lambda is two code paths. We contain it
with one scoring module (`common/scoring.py`) imported by both layers, and we *measure* the remaining
disagreement every day. Over a 43-day soak run the speed layer counted on average 0.54 % fewer readings
than the batch recompute (max 0.99 %, 0 missing windows), all of it explained by late data. The
consistency argument therefore rests on a number we can show rather than an assumption.
