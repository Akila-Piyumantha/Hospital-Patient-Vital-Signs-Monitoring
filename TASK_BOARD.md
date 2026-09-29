# Task Board

Tick a box when the task meets its acceptance criteria in [PROJECT_PLAN.md](PROJECT_PLAN.md) section 7 and its PR is merged (reviewed by a different member).
Milestones: **M1 = D5** walking skeleton · **M2 = D8** both layers integrated · **M3 = D12** code freeze · **D14** submit.

---

## Member A — Platform, Ingestion & Observability

**Name:** ________

### D1–D3
- [x] A1 Repo scaffold: `.env.example`, Makefile, `.gitignore`, lint config
- [x] A2 Docker Compose: Kafka, Postgres, Prometheus, Grafana, alerting, simulators (healthchecks, memory limits) — Spark/Airflow/API services are added by B/C in `compose/*.yml`
- [x] A3 `common/`: config loader, `sim_clock`, JSON logging helper, pydantic schemas
- [x] A5 `create_topics.sh` (`vitals.raw` x3 partitions, `vitals.dlq`)
- [ ] J1 Kickoff workshop attended, decisions confirmed
- [ ] J2 Contracts signed off

### D2–D7
- [x] A4 Vitals simulator (20 patients, deterioration episodes, spikes, fault injection, idempotent producer)
- [x] A6 Lab generator (one CSV per sim day, correlated with episodes, atomic drop, late/missing/corrupt option)
- [x] A7 `seed_patients.py`, Prometheus scrape config, kafka-exporter, producer metrics
- [ ] **M1 (D5)** skeleton demo

### D7–D10
- [x] A8 Alert rules + Alertmanager (13 rules total), each shown firing on the full stack (2026-09-29/30, live): NoVitalsData, SimulatorDown, ApiDown, StreamingQueryStopped, ScrapeTargetDown, ConsumerLagHigh, DlqRateHigh (16.2%, chaos profile), LabFileMissing, PipelineDataStale, SparkBatchSlow, DagFailed all captured in `data/alerts/alerts.jsonl`; SpeedBatchDiscrepancy verified by promtool unit test only (by design it stays under threshold — see docs/platform_observability.md §3.4). All 13 rules unit-tested (`make test-alerts`, promtool). Restart correctness re-verified live: 0 duplicate windows/alerts after killing spark-streaming.
- [x] A9 Grafana dashboards: ward live view + pipeline health (provisioned as code) — both provisioned, datasources healthy, and **every panel populated with live B/C data** (screenshots `docs/screenshots/pipeline-health-demos.png`, `ward-live.png`)
- [x] A10 Tests + GitHub Actions CI (simulator, sim_clock, schemas)
- [ ] **M2 (D8)** both layers integrated

### D11–D14
- [x] A11 `make e2e` smoke test + 30-min soak run — `python scripts/e2e_smoke.py --full` 10/10 checks pass on the live 17-service stack, both before and after a 40-minute full-stack soak (9 sim days, 0 alerts left firing, 3 990 MiB peak memory) that deliberately ran every A8 fault demo mid-soak; results in README and docs/platform_observability.md §4.2
- [ ] A12 README (architecture, setup, run, reproduce, troubleshooting) — written; needs B/C run sections and a clean-clone dry-run
- [ ] J5 Cross-review of a peer's module (viva prep)
- [ ] J6 Soak run, freeze, README dry-run on a clean clone
- [ ] Report: Ingestion, Tech stack, Observability chapters drafted in docs/platform_observability.md; ingestion/replay paragraph added to docs/speed_layer.md §1; architecture diagram still to redraw as a figure for the PDF (currently ASCII in README)
- [ ] J7/J8 Report assembly, proofread, own demo segment, contribution statement

---

## Member B — Speed layer (Spark Structured Streaming) & Lake

**Name:** ________

### D1–D5
- [x] B1 `common/scoring.py` with unit tests for every threshold boundary — done (PR #1)
- [x] B2 Spark container (connector versions pinned, JDBC driver, submit command in Compose) — done (`streaming/Dockerfile`, `compose/spark.yml`); built and run on the full stack
- [x] B3 Ingest and validate: schema, casting, range checks, DLQ, dedupe by `event_id` — done; live: input 21 255 = valid 20 633 + DLQ 419 + dropped 203
- [ ] J1 Kickoff workshop attended, decisions confirmed
- [x] J2 Contracts signed off; review `sql/init.sql` streaming tables — `sql/02_speed_layer.sql` confirmed by C, `init.sql` reviewed (identical `patient_lab_risk`); all B rows in docs/CHANGELOG.md agreed
- [ ] **M1 (D5)** skeleton demo

### D4–D8
- [x] B4 Enrichment: join with `patients`, MAP, pulse pressure, per-reading `news_score` — done (PR #1)
- [x] B5 Windowed aggregation (2 min / 30 s, 1 min watermark), trend slopes, sustained-abnormal counter — done; hand-computed fixture test green
- [x] B6 `foreachBatch` sinks: idempotent Postgres upserts + Parquet by `sim_day`, checkpoints, kill/restart test — done; kill -9 mid-batch → replay, no duplicates (docs/speed_layer.md §4.2)
- [ ] **M2 (D8)** both layers integrated

### D7–D11
- [x] B7 Alert engine: rules, severity, reason codes, cooldown, open/resolved lifecycle — done (PR #1)
- [x] B8 Lab feedback loop: stream-static join with `patient_lab_risk`, before/after evidence — done; verified with C's DAG on the full stack (P012 LOW → MEDIUM seconds after the day-4 run, docs/batch_serving.md §2.1); stand-in loader removed
- [x] B9 Streaming metrics via `StreamingQueryListener` + structured logs per micro-batch — done; also commits offsets to group `spark-speed-layer` for ConsumerLagHigh
- [x] B10 Unit tests + local-mode Spark integration test — done; 91 tests green in Linux container, CI job `spark-tests` added

### D10–D14
- [x] B11 Performance/tuning notes — measured, in docs/speed_layer.md §4
- [ ] J5 Cross-review of a peer's module (viva prep)
- [ ] J6 Soak run, freeze
- [ ] Report: Lambda vs Kappa chapter (with A's and C's paragraphs), processing layer, storage design — text in docs/speed_layer.md §1-3 incl. C's paragraph and the 41-day discrepancy evidence; A's paragraph pending
- [ ] J7/J8 Report assembly review, own demo segment, contribution statement

---

## Member C — Batch layer, Serving & Reporting

**Name:** Layanga Rajapakshe

### D1–D5
- [x] C1 `sql/init.sql`: all tables, keys, indexes, `pipeline_run_log` (B reviews) — done: batch tables + `batch_vitals_daily`, `speed_batch_reconciliation`; B's `02_speed_layer.sql` confirmed unchanged (docs/CHANGELOG.md); awaiting PR review
- [x] C2 Airflow setup: JDK + pyspark image, LocalExecutor, mounts, env connections — done (`airflow/Dockerfile`, `compose/airflow.yml`, FileSensor connection via env); built and running; verified live
- [x] C6 (part 1) FastAPI skeleton with `/api/ward/summary` and `/health` — done (see C6 part 2)
- [ ] J1 Kickoff workshop attended, decisions confirmed
- [ ] J2 Contracts signed off
- [ ] **M1 (D5)** skeleton demo

### D5–D10
- [x] C3 DAG `daily_lab_risk_report`: sensor → validate → load → lab risk → batch job → report → health check → archive, with retries, SLA, failure callback — done (`airflow/dags/`, logic in `batch/tasks.py`); live: days 1-6 green (scheduled + replays), missing-file run failed as designed and fired `LabFileMissing`
- [x] C4 Batch Spark job: full previous-day recompute from Parquet, speed-vs-batch discrepancy metric — done (`batch/daily_vitals_job.py`, window-level reconciliation → `speed_batch_discrepancy_ratio`); live: ~67 s/day, discrepancy 0.2-0.9 % per day
- [x] C5 Daily consolidated risk report (HTML + CSV + `patient_risk_report`, risk before vs after labs) — done (`batch/report.py`, sparklines, tier changes); retires B's `lab_risk_fixture.py`
- [x] C6 (part 2) Remaining endpoints: patients, vitals, alerts, reports, `/metrics`, pagination, OpenAPI — done (`serving/api/`, `compose/api.yml`); 12 API tests green against a test DB
- [x] C7 Failure and idempotency checks (replay a day, corrupt file, unknown patient) — automated tests + live runs of replay, corrupt file/unknown patient, bad schema and missing file (LabFileMissing fired) on 2026-09-29; screenshots `docs/screenshots/c7_*.png` (docs/batch_serving.md §4.3)
- [ ] **M2 (D8)** both layers integrated

### D6–D14
- [x] C8 Tests: lab parsing, risk merge, API with test DB, DAG import — done (`tests/batch`, `tests/serving`, `tests/airflow_dag`; CI jobs `batch-serving-tests`, `dag-import`)
- [ ] J5 Cross-review of a peer's module (viva prep)
- [ ] J6 Soak run, freeze
- [ ] C9 Report: use case and requirements, serving layer, results with screenshots, limitations; consistency paragraph in the architecture chapter — final text in docs/batch_serving.md §4 (results from the 43-day soak run, figures `docs/screenshots/c9_*.png`, `c7_*.png`); consistency paragraph added to docs/speed_layer.md §1; to be assembled into the report PDF (J7)
- [ ] J7/J8 Coordinate demo video, own demo segment, contribution statement

---

## Shared checkpoints (all three)

- [ ] D1 Kickoff (J1)
- [ ] D2 Contracts frozen (J2)
- [ ] D5 M1 walking skeleton demo (J3)
- [ ] D8 M2 two-layer integration (J4)
- [ ] D10–D12 Cross-reviews done (J5)
- [ ] D12 M3 code freeze, acceptance checklist ticked (J6)
- [ ] D13 Report PDF + demo video ready (J7, J8)
- [ ] D14 Submitted (repo link or zip, PDF, video)
