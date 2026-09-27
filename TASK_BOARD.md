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
- [ ] A8 Alert rules + Alertmanager (NoVitalsData, ConsumerLagHigh, DlqRateHigh, StreamingQueryStopped, LabFileMissing, ApiDown, DagFailed), each shown firing — rules written and validated; NoVitalsData, SimulatorDown, ApiDown, StreamingQueryStopped, ScrapeTargetDown demonstrated live; the rest need B/C metrics
- [ ] A9 Grafana dashboards: ward live view + pipeline health (provisioned as code) — both provisioned and datasources healthy; ward panels wait for B/C tables
- [x] A10 Tests + GitHub Actions CI (simulator, sim_clock, schemas)
- [ ] **M2 (D8)** both layers integrated

### D11–D14
- [ ] A11 `make e2e` smoke test + 30-min soak run — smoke test done; `--full` checks need B/C; soak run in progress
- [ ] A12 README (architecture, setup, run, reproduce, troubleshooting) — written; needs B/C run sections and a clean-clone dry-run
- [ ] J5 Cross-review of a peer's module (viva prep)
- [ ] J6 Soak run, freeze, README dry-run on a clean clone
- [ ] Report: Ingestion, Tech stack, Observability, architecture diagram, ingestion paragraph in the architecture chapter
- [ ] J7/J8 Report assembly, proofread, own demo segment, contribution statement

---

## Member B — Speed layer (Spark Structured Streaming) & Lake

**Name:** ________

### D1–D5
- [ ] B1 `common/scoring.py` with unit tests for every threshold boundary — done, awaiting PR review
- [ ] B2 Spark container (connector versions pinned, JDBC driver, submit command in Compose) — done (`streaming/Dockerfile`, `compose/spark.yml`), awaiting PR review
- [ ] B3 Ingest and validate: schema, casting, range checks, DLQ, dedupe by `event_id` — done; live: input 21 255 = valid 20 633 + DLQ 419 + dropped 203
- [ ] J1 Kickoff workshop attended, decisions confirmed
- [ ] J2 Contracts signed off; review `sql/init.sql` streaming tables — B's tables proposed in `sql/02_speed_layer.sql` (see docs/CHANGELOG.md), C to confirm
- [ ] **M1 (D5)** skeleton demo

### D4–D8
- [ ] B4 Enrichment: join with `patients`, MAP, pulse pressure, per-reading `news_score` — done, awaiting PR review
- [ ] B5 Windowed aggregation (2 min / 30 s, 1 min watermark), trend slopes, sustained-abnormal counter — done; hand-computed fixture test green
- [ ] B6 `foreachBatch` sinks: idempotent Postgres upserts + Parquet by `sim_day`, checkpoints, kill/restart test — done; kill -9 mid-batch → replay, no duplicates (docs/speed_layer.md §4.2)
- [ ] **M2 (D8)** both layers integrated

### D7–D11
- [ ] B7 Alert engine: rules, severity, reason codes, cooldown, open/resolved lifecycle — done, awaiting PR review
- [ ] B8 Lab feedback loop: stream-static join with `patient_lab_risk`, before/after evidence — done with a stand-in loader (`streaming/lab_risk_fixture.py`); re-check with C's DAG at M2
- [ ] B9 Streaming metrics via `StreamingQueryListener` + structured logs per micro-batch — done; also commits offsets to group `spark-speed-layer` for ConsumerLagHigh
- [ ] B10 Unit tests + local-mode Spark integration test — done; 91 tests green in Linux container, CI job `spark-tests` added

### D10–D14
- [ ] B11 Performance/tuning notes — measured, in docs/speed_layer.md §4
- [ ] J5 Cross-review of a peer's module (viva prep)
- [ ] J6 Soak run, freeze
- [ ] Report: Lambda vs Kappa chapter (with A's and C's paragraphs), processing layer, storage design — drafts in docs/speed_layer.md §1-3; A and C paragraphs pending
- [ ] J7/J8 Report assembly review, own demo segment, contribution statement

---

## Member C — Batch layer, Serving & Reporting

**Name:** ________

### D1–D5
- [ ] C1 `sql/init.sql`: all tables, keys, indexes, `pipeline_run_log` (B reviews)
- [ ] C2 Airflow setup: JDK + pyspark image, LocalExecutor, mounts, env connections
- [ ] C6 (part 1) FastAPI skeleton with `/api/ward/summary` and `/health`
- [ ] J1 Kickoff workshop attended, decisions confirmed
- [ ] J2 Contracts signed off
- [ ] **M1 (D5)** skeleton demo

### D5–D10
- [ ] C3 DAG `daily_lab_risk_report`: sensor → validate → load → lab risk → batch job → report → health check → archive, with retries, SLA, failure callback
- [ ] C4 Batch Spark job: full previous-day recompute from Parquet, speed-vs-batch discrepancy metric
- [ ] C5 Daily consolidated risk report (HTML + CSV + `patient_risk_report`, risk before vs after labs)
- [ ] C6 (part 2) Remaining endpoints: patients, vitals, alerts, reports, `/metrics`, pagination, OpenAPI
- [ ] C7 Failure and idempotency checks (replay a day, corrupt file, unknown patient)
- [ ] **M2 (D8)** both layers integrated

### D6–D14
- [ ] C8 Tests: lab parsing, risk merge, API with test DB, DAG import
- [ ] J5 Cross-review of a peer's module (viva prep)
- [ ] J6 Soak run, freeze
- [ ] C9 Report: use case and requirements, serving layer, results with screenshots, limitations; consistency paragraph in the architecture chapter
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
