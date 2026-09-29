# Platform, ingestion & observability — report chapters (Member A)

Report-chapter drafts for task A's sections: use-case interpretation of the ingestion side, technology
stack justification, and observability design. Code: [simulators/](../simulators),
[common/](../common) (shared by all three layers), [observability/](../observability). The
architecture-decision chapter is Member B's, with a paragraph from A on ingestion/replay
([docs/speed_layer.md §1](speed_layer.md#1-architecture-decision-lambda-not-kappa)).

---

## 1. Ingestion layer

### 1.1 What the two simulated sources represent

`simulators/vitals_producer.py` stands in for 20 bedside monitors. Each patient has a physiological
baseline (heart rate, SpO₂, systolic/diastolic BP, temperature) drawn once at start-up from a seeded
random cohort (`simulators/patients.py`), then perturbed every 2 s by mean-reverting noise (an AR(1)
process per vital) plus, for four scripted patients, a deterioration episode (sepsis-like or
respiratory) that ramps, holds and recovers. Two further patients are *occult*: their vitals never
leave the normal range, but their labs turn abnormal from a fixed simulated day on. This is not
decoration — it is what makes the business question ("which patients are concerning right now, and how
do labs change that picture") answerable at all: without a scripted, known-in-advance ground truth
(`data/ground_truth/episodes.json`), there would be no way to check that the speed layer's alerts, the
lab-risk feedback loop, or the daily report actually catch the right patients rather than just running
without crashing.

`simulators/lab_generator.py` stands in for the pathology lab's once-a-day extract: one CSV per
simulated day, correlated with the same scripted story (abnormal lactate/WBC/CRP while a sepsis episode
is active, abnormal creatinine/potassium/glucose for chronic conditions, abnormal results for the occult
patients from their trigger day). Both sources are plain Python processes — no framework — which keeps
them auditable and lets a reader trace every number in a report back to a line of generator code.

### 1.2 Robustness built into the sources (data-ingestion marks)

* **Idempotent, ordered delivery.** The Kafka producer runs with `enable.idempotence=True` and
  `acks=all`, so a broker-side retry cannot itself manufacture the duplicates the pipeline is built to
  tolerate; messages are keyed by `patient_id`, so Kafka's own partition ordering guarantees are enough
  for the speed layer's trend calculations without an extra sequencing mechanism.
* **Deliberate fault injection**, not just the happy path (`simulators/faults.py`): null vitals,
  physiologically impossible values, duplicate `event_id`s, events held back 20–90 s past their
  timestamp, and short sensor dropouts, at a configurable rate (`FAULT_PROFILE=off|low|chaos`). This
  exists specifically so the downstream validation, deduplication, watermark handling and alerting have
  something real to prove themselves against — see §3.4 for evidence that it does.
* **Atomic file delivery.** The lab file is written to a `.tmp` path and `os.replace`d into place, so a
  consumer globbing `labs_day_*.csv` can never observe a half-written file — a real failure mode when a
  "daily extract" is modelled as a plain file drop.
* **A shared, race-free simulated clock** (`common/sim_clock.py`): both sources derive `sim_day` from
  one epoch, agreed by whichever process starts first via an `O_EXCL` file create (tested under
  concurrent start-up, `tests/common/test_sim_clock.py`), so "day N" means the same thing to the
  streaming source, the batch source, the speed layer and the batch layer without any of them talking to
  each other directly.
* **Reproducibility.** Every random choice is seeded (`SIM_SEED`), so the same seed always produces the
  same cohort, the same episodes and the same lab values — a prerequisite for the fixture-based tests
  Members B and C wrote against this data.

### 1.3 Kafka topic design

`vitals.raw` has **3 partitions keyed by `patient_id`**, `vitals.dlq` has 1. Three partitions match
`spark.sql.shuffle.partitions` on the speed layer (`docs/speed_layer.md §4`) — more would not add
parallelism with 20 patients and a single-broker cluster, fewer would leave a Spark task idle.
Retention is short (24 h / 256 MB) because Kafka here is a transport, not the system of record: the
Parquet lake is (contract 4.3, owned by B). The full argument for why replay reads the lake and not the
topic is in [docs/speed_layer.md §1](speed_layer.md#1-architecture-decision-lambda-not-kappa).

---

## 2. Technology stack — justification tied to this use case

| Layer | Choice | Why this use case needs it (not generic popularity) |
|---|---|---|
| Ingestion | **Kafka**, 3 partitions keyed by patient | Per-patient ordering is a correctness requirement for trend slopes (§2.1 in speed_layer.md), not a nice-to-have; a topic (vs. calling an HTTP endpoint) decouples 20 independent bedside monitors from a speed layer that may be temporarily down, and gives the speed layer something to resume from after a restart. |
| Stream processing | **Spark Structured Streaming** | The business question needs *event-time* windows and watermarks over out-of-order sensor data (late readings, simulated on purpose) plus a stream-static join against daily lab risk that must pick up new rows without a restart — both are native to Structured Streaming. Storm was rejected: it would need hand-rolled state management for the same windowing and joins this project needs out of the box. |
| Orchestration | **Airflow** | The lab side is a genuine multi-step batch dependency chain (wait for file → validate → load → score → recompute → report → check → archive) with retries and a visible run history — exactly Airflow's job, and the DAG in this project is not a token gesture: it is the entire batch half of the Lambda architecture (docs/batch_serving.md). |
| Storage/serving | **PostgreSQL** (speed + batch tables) and **Parquet on a local volume** (the lake, standing in for S3/HDFS) | Postgres: the write pattern is small, frequent *upserts* (≈20 patient rows + ≈100 window rows every 5 s) with ad-hoc SQL joins across speed and batch tables — a poor fit for Cassandra's append-oriented, join-averse model at this scale. Parquet: the lake must be cheaply re-scannable by simulated day for the batch recompute and for replay; a columnar, partition-pruned format is the natural fit, and the brief explicitly allows a file system + Parquet in place of a database for this role. |
| Observability | **Prometheus + Alertmanager + Grafana**, plus structured JSON logs | Every component here (simulators, Spark, Airflow, the API) is a different language/runtime; Prometheus's pull model and a common metric-naming contract (PROJECT_PLAN.md §4.7) let one dashboard and one alert-rule set cover all of them without each component needing to know about the others. |
| Packaging | **Docker Compose**, one file per layer joined by `include:` | Three people each own a compose fragment (`compose/spark.yml`, `compose/airflow.yml`, `compose/api.yml`) without touching the same file — a direct answer to "three members working in parallel" rather than a generic choice. |

---

## 3. Observability design

### 3.1 What is measured, and why

| Signal | Instrument | Answers |
|---|---|---|
| Is data still arriving? | `simulator_last_emit_timestamp_seconds`, `kafka_topic_partition_current_offset`, `pipeline_last_event_age_seconds` | "Has the pipeline gone silent?" — the single most important health question for a monitoring system that is itself supposed to raise alarms. |
| Is the speed layer keeping up? | `kafka_consumergroup_lag`, `spark_batch_duration_seconds`, `spark_input_rows_per_sec` | "Is processing falling behind ingestion?" — a lagging speed layer means stale ward risk tiers. |
| Is data quality holding? | `vitals_valid_total`, `vitals_dlq_total{reason}`, `vitals_faults_injected_total{type}` | "What fraction of readings are being rejected, and why?" — ties every DLQ record to a documented cause instead of a silent drop. |
| Is the batch layer running on schedule? | `airflow_dag_last_success_timestamp{dag_id}`, `lab_file_missing_total` | "Did today's report actually get produced?" — the daily report is a deliverable, not a side effect. |
| Do the two Lambda views agree? | `speed_batch_discrepancy_ratio` | Turns "the batch layer exists to catch what the speed layer misses" from an architectural claim into a number that is checked every simulated day (docs/speed_layer.md §1, §4.2). |
| Is the ward-facing surface healthy? | `api_request_duration_seconds`, `/health` (503 on stale data) | The consolidated report/dashboard is the deliverable the brief asks for; "the API responds" and "the API's data is fresh" are checked separately, because they fail independently. |

### 3.2 Alert rules (13, all unit-tested)

Rules live as code in [observability/alert_rules.yml](../observability/alert_rules.yml), one rule per
row of the table above plus liveness checks (`SimulatorDown`, `StreamingQueryStopped`, `ApiDown`,
`ScrapeTargetDown`) and two consistency guards (`SpeedBatchDiscrepancy`, `PipelineDataStale`). Every
rule is unit-tested against synthetic time series with `promtool test rules`
([observability/alert_rules_test.yml](../observability/alert_rules_test.yml), `make test-alerts`, run
in CI) — this matters specifically because several rules depend on metrics Members B and C own
(`kafka_consumergroup_lag{consumergroup="spark-speed-layer"}`, `airflow_dag_last_success_timestamp`,
`speed_batch_discrepancy_ratio`): the rules were verified correct *before* those components existed, so
integration was a matter of confirming names (docs/CHANGELOG.md) rather than debugging alert logic
under time pressure.

Alerts route through Alertmanager to a small webhook (`observability/alert_webhook/`) that turns each
`firing`/`resolved` transition into a structured log line and appends it to
`data/alerts/alerts.jsonl` — a durable, greppable record of every incident for the report and the
demo, independent of Prometheus's own retention.

### 3.3 Structured logging

Every service (Python or JVM) writes one JSON object per stdout line via a shared formatter
(`common/logging_setup.py`): `{"ts","level","service","stage","event","run_id", ...}`, where `stage` ∈
`ingestion|processing|storage|serving|orchestration|observability`. This means `docker compose logs |
grep '"stage": "ingestion"'` isolates one pipeline stage across every container without any
service-specific knowledge, and every log line carries a `run_id` for correlating a whole container
lifetime. Third-party log lines (e.g. librdkafka's internal retries) are routed through the same
formatter with a default stage, so nothing on stdout falls outside the contract.

### 3.4 Results: the alert chain demonstrated live

All of the following were captured on the full stack (every service of A, B and C running together)
on 2026-09-29/30. Figures in `docs/screenshots/`.

| Alert | How it was triggered | Result |
|---|---|---|
| `StreamingQueryStopped` + `ConsumerLagHigh` | `docker compose stop spark-streaming` while vitals kept flowing | `StreamingQueryStopped` fired within the 1-minute `for`; consumer lag climbed from ~130 to ~1 800 messages and `ConsumerLagHigh` fired above the 500-message threshold ~50 s later. Restarting the job drained the lag back to baseline within two minutes (Fig. A-1, `pipeline-health-demos.png`, "Consumer lag" panel) with **no duplicate windows and no duplicate alerts** after the restart (`SELECT ... GROUP BY patient_id, window_start HAVING count(*) > 1` → 0 rows; `count(*) = count(DISTINCT alert_id)` on `alerts`), confirming the checkpoint-based recovery Member B designed. |
| `DlqRateHigh` | `FAULT_PROFILE=chaos docker compose up -d vitals-simulator` | The DLQ ratio climbed from the baseline ~2 % to 16.2 % in under two minutes; `DlqRateHigh` went `pending` at the 5 % crossing and `firing` a minute later, logged as `"DLQ ratio is 15.92%"` in `data/alerts/alerts.jsonl`. Reverting to `FAULT_PROFILE=low` let the ratio fall back under threshold and the alert resolve. The "Injected faults by type" and "DLQ share of readings" panels show the same spike (Fig. A-1). |
| `LabFileMissing` | `LAB_FORCE_MISSING_DAYS=<n>` (Member C, docs/batch_serving.md §2.1/§4.3) | Sensor timeout → `lab_file_missing_total` incremented → alert firing in Prometheus within seconds → webhook → `alerts.jsonl`, end-to-end chain screenshotted in `c7_missing_file_evidence.png`. |
| `DagFailed` | `docker compose stop airflow-scheduler` for over 11 minutes | `airflow_dag_last_success_timestamp` (pushed to the Pushgateway, so it survives the scheduler being down) kept ageing; the alert went `pending` at the 660 s crossing and `firing` 30 s later, logged as `"Last success 11m 41s ago"`. On restart the scheduler picked up its backlog automatically — no manual intervention — running the missed `20:55` interval and then the next one in sequence; `DagFailed` resolved as soon as the backlog run reported success (`airflow dags list-runs`: `scheduled__2026-09-29T20:55:00` → `success`). |
| `NoVitalsData` / `SimulatorDown` | `docker compose stop vitals-simulator` | Fired after 45 s / 30 s respectively and resolved on restart (first captured during the platform-only soak run, §4; reconfirmed in this session's `alerts.jsonl`). |
| `ApiDown`, `ScrapeTargetDown` | service naturally absent/stopped | Fired as designed; `ScrapeTargetDown` is a deliberately generic catch-all for any of the lighter services (kafka-exporter, pushgateway, alertmanager, lab-generator, vitals-simulator). |
| `SparkBatchSlow` + `PipelineDataStale` | occurred naturally when the batch Spark job and the streaming job contended for CPU on one laptop (2026-09-29, `data/alerts/alerts.jsonl`) | `SparkBatchSlow` fired when micro-batches held above 10 s; `PipelineDataStale` fired at the same time on both the Spark and API instances ("Newest processed reading is 1m 50s old") and both resolved once the contention passed — direct evidence that the two alerts catch a real resource-contention scenario, not just an artificial one. |
| `SpeedBatchDiscrepancy` | *(designed to stay quiet; not deliberately triggered)* | Stayed at 0.19–0.99 % through normal operation, an order of magnitude under the 10 % threshold — itself the evidence for the Lambda consistency argument (docs/speed_layer.md §1). It briefly touched **2.5 %** while `airflow-scheduler` was down for the `DagFailed` demo (the backlog run recomputed a day whose speed-layer windows were still catching up from the earlier `spark-streaming` restart) and fell back under 1 % once both layers were caught up — still 4× under threshold, and a real illustration of *why* the metric exists. Verified independently by a `promtool` unit test with a synthetic 15 % series. |

Fig. A-1 (`pipeline-health-demos.png`) captures the whole 40-minute demo sequence on one dashboard: the
`spark-streaming` restart (Kafka/producer dip, consumer-lag spike and drain), the `FAULT_PROFILE=chaos`
DLQ spike, the micro-batch slowdown and discrepancy blip while Spark and the backlogged batch job
competed for CPU, and the "seconds since last DAG success" sawtooth climbing to its `DagFailed` peak and
resetting on recovery — ending with 0 firing alerts and every metric back to baseline. Fig. A-2
(`ward-live.png`) is the ward-facing view over the same live stack, showing the risk-tier table and a
lab-driven tier change on the daily report (patients P005/P012 rising to MEDIUM, P018 dropping to LOW).

### 3.5 Design choices worth defending in the viva

* **Rejection marks, not silent drops.** Every rejected reading gets a `reason` label
  (`vitals_dlq_total{reason="out_of_range:heart_rate"}`, …) rather than one opaque counter, so the DLQ
  ratio alert can be explained by cause, not just by magnitude.
* **`absent()` guards where "no data" is itself the failure** (`NoVitalsData`) but **not** on
  `DagFailed`: before the very first DAG success there is no time series to be "absent", and guarding it
  would make every fresh `docker compose up` open a false alert during the first simulated day while
  Airflow is still initialising. This is a considered trade-off, not an oversight — noted here so it can
  be defended in the viva.
* **A webhook instead of email/Slack.** Alertmanager can route anywhere; a local webhook that logs to a
  file needs no external account, keeps the whole alert history reproducible from `data/`, and is enough
  to prove the pattern generalises to a real notification channel.

---

## 4. Soak-run evidence

### 4.1 Platform-only soak (2026-09-25, before B/C existed)

See [README.md §"Soak run (measured)"](../README.md#soak-run-measured) for the full table: 30.4 minutes,
7 simulated days, 9.2 msg/s average, 0 restarts, 779 MiB peak memory across the (then much smaller)
stack.

### 4.2 Full-stack soak (2026-09-29/30, every service of A, B and C)

`scripts/soak_monitor.py --minutes 40` against the full 17-service stack, with every alert-chain
demonstration of §3.4 deliberately run *during* the window rather than around it — a harder test than a
quiet run, because it asks the stack to keep producing correct data, correct lab files and a correct
daily report while three of its own components were being killed and restarted on purpose.

| Metric | Result |
|---|---|
| Duration / simulated days covered | 40.5 min / 9 days |
| Kafka throughput (msgs/s) | min 3.5 (during the `FAULT_PROFILE=chaos` window) · avg 8.9 · max 9.6 |
| Lab files landed | 46, none missing |
| Container restarts | 1 (`spark-streaming` — the deliberate `ConsumerLagHigh`/`StreamingQueryStopped` demo, §3.4) |
| Peak memory, all 17 containers | 3 990 MiB (comfortably inside an 8 GB Docker Desktop allocation) |
| Alerts firing at the end of the run | **0** — every alert opened during the run (`ConsumerLagHigh`, `DlqRateHigh`, `DagFailed`, `SparkBatchSlow`, `PipelineDataStale`) had resolved |
| Full-stack smoke test, before and after the soak | `python scripts/e2e_smoke.py --full`: **10/10** checks pass both times, confirming the stack recovered cleanly from all three deliberate outages |

The platform-only run (§4.1) shows the baseline; this run shows the same platform holding together while
the two heavier layers (Spark, Airflow) are added and then deliberately disrupted — the combination the
marking rubric's "detect and diagnose pipeline failures" criterion is really asking for.
