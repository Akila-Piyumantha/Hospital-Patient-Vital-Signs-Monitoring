# Hospital Patient Vital Signs Monitoring

End-to-end **Lambda-architecture** data pipeline (EC8203 Applied Big Data Engineering mini-project,
use case 2): near-real-time bedside vital signs are correlated with the pathology lab's once-a-day
results to answer

> *Which patients show concerning vital-sign trends right now, and how do yesterday's lab results
> change the risk picture for those patients going forward?*

Full design, task split and contracts: [PROJECT_PLAN.md](PROJECT_PLAN.md) · progress: [TASK_BOARD.md](TASK_BOARD.md) ·
contract changes: [docs/CHANGELOG.md](docs/CHANGELOG.md)

> **Status:** all three layers (A: platform/ingestion/observability, B: Spark speed layer, C: Airflow batch
> layer/API/daily report) are implemented and verified running together (`docker compose up -d --build`,
> 17 services) — `python scripts/e2e_smoke.py --full` passes all 10 checks on the live stack.

## Architecture

```
 vitals-simulator ──► Kafka  vitals.raw (3 partitions, key = patient_id) ──► Spark Structured Streaming ─┐  speed layer
   (20 patients, 1 msg/2 s)        └─► vitals.dlq (rejected records)              │ upserts               │
                                                                                   ▼                       │
 lab-generator ──► data/landing/labs/labs_day_NNN.csv ──► Airflow DAG ──► PostgreSQL ◄─────────────────────┘
   (1 file per simulated day)                              (batch layer)      ▲  + Parquet lake (data/lake)
                                                                              │
                                                             FastAPI  +  Grafana dashboards  +  daily report

 Observability:  JSON logs (stdout) · Prometheus metrics · Alertmanager ─► alert-webhook ─► data/alerts/alerts.jsonl
```

**Why Lambda, not Kappa** (argued in the report; summary): labs are inherently batch (one file a day,
possibly late or corrected), the daily risk report is a full recompute that must be correct and
replayable independent of Kafka retention, while the vitals need seconds-level alerts. The batch layer
also *measures* the speed layer's approximation error (`speed_batch_discrepancy_ratio`).

### Simulated clock

**1 simulated day = 5 real minutes** (`SIM_DAY_SECONDS=300`). `sim_day = floor((now − epoch)/300) + 1`;
the epoch is recorded in `data/state/sim_epoch` by whichever service starts first. Each patient emits one
reading every **2 s** (≈10 msg/s, ≈3 000 per simulated day). The lab file for day *N* holds the labs
collected on day *N−1* ("yesterday") and is dropped at the start of day *N*; day 1's file is a baseline
from a virtual day 0. A 30-minute run therefore covers 6 simulated days.

## Quick start

Requirements: Docker Desktop (≥ 8 GB RAM for WSL2; the platform layer measured ≈ 0.7-0.8 GB in use, hard limits total ≈ 3.5 GB), nothing else.

```bash
docker compose up -d --build     # or: make up          (first build takes a few minutes)
docker compose ps                # all services running / healthy
docker compose logs -f vitals-simulator lab-generator     # JSON logs
```

| What | URL |
|---|---|
| Grafana (dashboards, no login for viewing; admin/admin to edit) | http://localhost:3000 |
| Prometheus (targets, alert rules, graph) | http://localhost:9090 |
| Alertmanager | http://localhost:9093 |
| Simulator metrics | http://localhost:8001/metrics · http://localhost:8002/metrics |
| Kafka (from the host) | `localhost:29092` |
| Postgres | `localhost:5432` (user/password/db: `hospital`) |
| Pushgateway (batch job metrics) | http://localhost:9091 |
| Airflow (admin / admin) | http://localhost:8080 |
| Serving API (OpenAPI docs) | http://localhost:8000/docs |

Stop with `docker compose down`; wipe everything (volumes + generated data, restarting simulated time at
day 1) with `make reset` or:

```bash
docker compose down -v
rm -rf data/state data/landing data/lake data/checkpoints data/reports data/alerts data/ground_truth data/soak
```

Configuration: everything is an environment variable with a default; copy `.env.example` to `.env`
to change it (topic names, day length, patient count, fault rates, …).

### Verify it works

```bash
pip install -r requirements-dev.txt         # once, for the host-side checks
python scripts/e2e_smoke.py --wait 120      # platform + ingestion checks
python scripts/e2e_smoke.py --full          # + speed layer, batch layer, API (once B and C land)
```

Useful manual checks:

```bash
# tail the stream
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic vitals.raw --from-beginning --max-messages 5
# partitions and offsets
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic vitals.raw
# patients in the database
docker compose exec postgres psql -U hospital -c "SELECT patient_id, bed, comorbidity FROM patients LIMIT 5"
# alert trail
cat data/alerts/alerts.jsonl
```

### Soak runs (measured)

`python scripts/soak_monitor.py --minutes N` samples the stack every 30 s into `data/soak/soak.csv` and
writes `data/soak/summary.json`.

#### Platform layer alone (2026-09-25, before B/C existed)

Simulators + Kafka + Postgres + observability only, `FAULT_PROFILE=low`:

| Metric | Result |
|---|---|
| Duration / simulated days covered | 30.4 min / 7 days |
| Kafka throughput (msgs/s) | min 8.1 · avg 9.2 · max 9.7 (20 patients × 1 per 2 s = 10 nominal; the gap is injected sensor dropouts and held-back late events) |
| Lab files landed | 7 (one per simulated day, none missing) |
| Container restarts | 0 |
| Peak memory, all containers | 779 MiB |
| Alerts firing | only `ApiDown`, `StreamingQueryStopped` (B/C services not deployed yet) |

Live check against the ground truth: patient P001's scripted sepsis episode showed in Kafka as HR 74→108,
SpO₂ 96→91, systolic BP 122→85, temperature 36.6→38.9 °C within one 4-minute ramp, and the following lab
file listed lactate 2.7, WBC 14.3 and CRP 58.7 as abnormal for P001.

#### Full stack (2026-09-29/30, all 17 services of A, B and C, including deliberate fault demos)

| Metric | Result |
|---|---|
| Duration / simulated days covered | 40.5 min / 9 days |
| Kafka throughput (msgs/s) | min 3.5 · avg 8.9 · max 9.6 (the minimum is the `FAULT_PROFILE=chaos` window, §"Fault injection", not a fault) |
| Lab files landed | 46 (one per simulated day, none missing) |
| Container restarts | 1 (`spark-streaming`, from the deliberate `ConsumerLagHigh` demo below - not a crash) |
| Peak memory, all 17 containers | 3 990 MiB |
| Alerts fired live during this run | `ConsumerLagHigh`, `DlqRateHigh`, `DagFailed`, `SparkBatchSlow`, `PipelineDataStale` - all deliberately triggered (see next section) and all resolved by the end of the run; 0 alerts left firing |

Full narrative of what was triggered, why, and the evidence for each: [docs/platform_observability.md
§3.4](docs/platform_observability.md#34-results-the-alert-chain-demonstrated-live).

## What the sources simulate

**Vitals stream** (`simulators/vitals_producer.py`) - per patient a physiological model (baseline +
mean-reverting noise) with three ingredients that make the business question answerable:

* *deteriorating* patients (4): a scripted sepsis-like or respiratory episode ramps HR/SpO₂/BP/temperature
  towards danger, holds, recovers, and repeats milder later;
* *occult* patients (2): vitals stay normal, but their labs turn abnormal from sim day 2-3 - only the
  daily lab join reveals their risk;
* random short **spikes** (1 % per reading) for everyone.

Robustness features: idempotent producer (`acks=all`, retries, back-off), key = `patient_id`,
delivery callbacks with error metrics, broker/topic wait on start, graceful SIGTERM flush, seeded RNG
(reproducible), drift-free scheduling, and **fault injection** (below).

**Lab file** (`simulators/lab_generator.py`) - `labs_day_NNN.csv` with
`patient_id,test_type,result_value,reference_range,collected_at` for potassium, creatinine, lactate, WBC,
CRP, haemoglobin and glucose. Abnormal results follow the patients' story (episodes, chronic conditions,
occult risk). Files are written atomically (`*.tmp` → rename).

The hidden story is saved to `data/ground_truth/episodes.json` (who deteriorates when) so alerts and lab
risk can be validated against it. It is deliberately *not* in the database.

### Fault injection & alert demonstrations

| Demonstration | How | Expected |
|---|---|---|
| Invalid / duplicate / late / missing readings | `FAULT_PROFILE=low` (default) | ~2 % rejected → DLQ, duplicates de-duplicated, late events exercise the watermark |
| **DlqRateHigh** | `FAULT_PROFILE=chaos docker compose up -d vitals-simulator` | > 5 % rejected → alert after ~1 min |
| **NoVitalsData** | `docker compose stop vitals-simulator` | offsets stop advancing → alert after ~45 s (also `SimulatorDown`) |
| **LabFileMissing** | `LAB_FORCE_MISSING_DAYS=4` in `.env`, `docker compose up -d lab-generator` | file for day 4 never appears → sensor times out → alert |
| Late / corrupt / bad-schema lab file | `LAB_FORCE_LATE_DAYS`, `LAB_FORCE_CORRUPT_DAYS`, `LAB_FORCE_BADSCHEMA_DAYS` | DAG validation must delay / clean / quarantine |
| **ConsumerLagHigh** | `docker compose stop spark-streaming`, wait | lag grows → alert |
| **StreamingQueryStopped / ApiDown / DagFailed** | stop the respective service | see `observability/alert_rules.yml` |

Alerts appear in Prometheus (Alerts tab), the *Pipeline Health* dashboard, `docker compose logs alert-webhook`
and `data/alerts/alerts.jsonl`.

## Speed layer (Spark Structured Streaming)

Service `spark-streaming` ([compose/spark.yml](compose/spark.yml), code in [streaming/](streaming/)).
Two streaming queries read `vitals.raw`:

| Query | What it does | Writes |
|---|---|---|
| `vitals_readings` | parse → validate (rejection reason) → join `patients` + `patient_lab_risk` → MAP, pulse pressure, NEWS, total score, tier → dedupe `event_id` | `vitals.dlq` + `dlq_events`, Parquet lake `data/lake/vitals/sim_day=N/`, `patient_status`, reading alerts |
| `vitals_windows` | dedupe (1-min watermark) → 2-min windows sliding every 30 s on **event time**, update mode | `vitals_window`, trend slopes / flag, sustained counter, window alerts |

Scoring rules shared with the batch layer live in [common/scoring.py](common/scoring.py).
Metrics: http://localhost:8003/metrics · Spark UI (Structured Streaming tab): http://localhost:4040.

```bash
docker compose up -d --build spark-streaming      # also starts its dependencies
docker compose logs -f spark-streaming | grep -E 'readings_batch_written|alert_opened|risk_tier_changed'
docker compose exec postgres psql -U hospital -c \
  "SELECT patient_id, risk_tier, news_score, lab_risk_points, trend_flag, last_reading_at FROM patient_status ORDER BY total_score DESC LIMIT 8"
docker compose exec postgres psql -U hospital -c \
  "SELECT opened_at, patient_id, reason_code, severity, resolved_at FROM alerts ORDER BY opened_at DESC LIMIT 10"
docker compose exec postgres psql -U hospital -c "SELECT reason, count(*) FROM dlq_events GROUP BY 1"
```

**Lab feedback loop:** when the Airflow DAG writes `patient_lab_risk` (every sim day, or `make replay-day DAY=N`),
the job picks the new rows up within one trigger (5 s) and logs `lab_risk_applied` / `risk_tier_changed` for
patients whose lab points changed: `docker compose logs spark-streaming | grep -E 'lab_risk_applied|risk_tier_changed'`.

**Restart / exactly-once check:** `docker compose restart spark-streaming`, then
`SELECT patient_id, window_start, count(*) FROM vitals_window GROUP BY 1,2 HAVING count(*) > 1` (no rows) and
`SELECT count(*), count(DISTINCT alert_id) FROM alerts` (equal). The query resumes from its checkpoint (named volume
`spark-checkpoints`; `docker compose down -v` / `make reset` clears it).

**Tests:** `pytest tests/common/test_scoring.py tests/streaming` - pure-Python tests always run; the Spark tests need
`pip install -r streaming/requirements.txt` and Java 17 (the streaming/Parquet ones are skipped on Windows, which lacks
`winutils.exe`; they run in CI job `spark-tests` or in the container:
`docker compose run --rm --no-deps -v "$PWD/tests:/app/tests" -v "$PWD/simulators:/app/simulators" spark-streaming sh -c "pip install -q pytest && python -m pytest -q -p no:cacheprovider tests/streaming tests/common/test_scoring.py"`).

Design, tuning notes and the report chapters: [docs/speed_layer.md](docs/speed_layer.md).

## Batch layer (Airflow) and serving API

Services `airflow-init`, `airflow-webserver`, `airflow-scheduler` ([compose/airflow.yml](compose/airflow.yml)) and
`api` ([compose/api.yml](compose/api.yml)); code in [batch/](batch/), [airflow/dags/](airflow/dags/),
[serving/api/](serving/api/); schema [sql/init.sql](sql/init.sql).

| What | URL |
|---|---|
| Airflow UI (graph view, run history; admin / admin) | http://localhost:8080 |
| API docs (OpenAPI, try it out) | http://localhost:8000/docs |
| Ward summary | http://localhost:8000/api/ward/summary |
| Latest daily risk report (JSON / HTML) | http://localhost:8000/api/reports/risk/latest · `/api/reports/risk/<day>/html` |

DAG `daily_lab_risk_report` runs once per simulated day: `wait_for_lab_file` (FileSensor) → `validate_lab_file`
(bad rows / files → `data/landing/labs/quarantine/`) → `load_lab_results` → `compute_lab_risk` (→ `patient_lab_risk`,
picked up by the speed layer within one trigger) → `batch_vitals_job` (Spark recompute of the previous day from the
lake + speed-vs-batch reconciliation) → `build_risk_report` (`patient_risk_report` + `data/reports/risk_report_day_NNN.{html,csv}`)
→ `data_quality_and_health_check` → `archive_file`. Metrics go to the Pushgateway (`airflow_dag_last_success_timestamp`,
`lab_file_missing_total`, `speed_batch_discrepancy_ratio`, …).

```bash
docker compose up -d --build airflow-scheduler airflow-webserver api     # plus their dependencies
docker compose logs -f airflow-scheduler | grep '"service": "airflow-batch"'   # JSON events of the DAG tasks
make replay-day DAY=3        # re-run one day (idempotent); = airflow dags trigger ... -c '{"sim_day": 3}'
make dag-runs                # run history
curl -s localhost:8000/api/ward/summary | python -m json.tool
curl -s "localhost:8000/api/patients?risk_tier=HIGH"
curl -s "localhost:8000/api/alerts?status=open&severity=CRITICAL"
docker compose exec postgres psql -U hospital -c \
  "SELECT rank, patient_id, risk_before_labs, risk_after_labs, lab_summary FROM patient_risk_report
   WHERE sim_day = (SELECT max(sim_day) FROM patient_risk_report) ORDER BY rank LIMIT 8"
docker compose exec postgres psql -U hospital -c "SELECT * FROM speed_batch_reconciliation ORDER BY sim_day"
```

Day numbering: the run for day N loads `labs_day_N.csv` (labs collected on day N-1), writes lab risk `as_of_sim_day = N`,
recomputes the vitals of day N-1 and produces the report for day N. Failure handling (replay, corrupt / bad-schema /
missing file), the reconciliation formula and the report chapter drafts: [docs/batch_serving.md](docs/batch_serving.md).

**Tests:** `pytest tests/batch tests/serving` - pure-Python tests always run; the database tests use a throw-away
database on the Postgres at `localhost:5432` (e.g. the Compose one; override with `TEST_POSTGRES_HOST`/`_PORT`/`_USER`/`_PASSWORD`)
and are skipped when none is reachable; API tests need `pip install -r serving/requirements.txt`; the Spark test needs
`pip install -r airflow/requirements.txt` and Java 17. The DAG import test (`tests/airflow_dag`) needs Airflow and runs in
CI job `dag-import`.

## Observability

* **Structured logging** - one JSON object per line on stdout from every service:
  `{"ts","level","service","stage","event","run_id", …}`; `stage` ∈ ingestion | processing | storage |
  serving | orchestration | observability, so `docker compose logs | grep '"stage": "ingestion"'`
  isolates a pipeline stage. Shared helper: [common/logging_setup.py](common/logging_setup.py).
* **Metrics** - Prometheus scrapes the simulators, kafka-exporter (topic offsets, consumer lag),
  Spark (`spark-streaming:8003`), API (`api:8000`) and the Pushgateway (Airflow/batch jobs). Metric
  names are contract 4.7 in the plan.
* **Alerts** - rules as code in [observability/alert_rules.yml](observability/alert_rules.yml)
  (no-data, error-rate, consumer lag, stale data, DAG failure, missing lab file, speed/batch
  discrepancy, target down), routed by Alertmanager to a webhook that logs and persists them.
  The rules are unit-tested with synthetic time series
  ([observability/alert_rules_test.yml](observability/alert_rules_test.yml), `make test-alerts`), so rules that
  depend on Spark/Airflow/API metrics are verified before those components exist.
* **Dashboards** - provisioned at start-up: *Pipeline Health* (Prometheus) and *Ward Live Monitoring*
  (Postgres). Regenerate the JSON with `make dashboards`
  ([observability/grafana/build_dashboards.py](observability/grafana/build_dashboards.py)).

Design, alert-chain evidence and report chapters (ingestion, tech stack, observability):
[docs/platform_observability.md](docs/platform_observability.md).

## Repository layout

```
common/            config, simulated clock, JSON logging, data-contract models   (A)
simulators/        vitals producer, lab generator, patient model, faults, seeding (A)
kafka/             topic creation script                                          (A)
sql/               00_ airflow DB · 01_ patients table · 02_ speed-layer tables (B) · init.sql (C: batch tables)
streaming/         Spark Structured Streaming job                                  (B)
batch/             lab pipeline, batch Spark job, daily report, DAG task callables (C)
airflow/           Airflow image + dags/daily_lab_risk_report.py                  (C)
serving/           FastAPI serving layer                                          (C)
compose/           spark.yml (B) · airflow.yml (C) · api.yml (C), included by docker-compose.yml
observability/     prometheus, alert rules, alertmanager, webhook, Grafana        (A)
scripts/           e2e_smoke.py
tests/             pytest suite (run: make test)
data/              runtime data (git-ignored): landing/, lake/, alerts/, state/, ground_truth/
```

## Development

```bash
python -m venv .venv && . .venv/Scripts/activate   # Windows Git Bash; use .venv/bin/activate elsewhere
pip install -r requirements-dev.txt
pytest                                             # unit tests (no Docker needed)
ruff check . && ruff format --check .
python -m simulators.vitals_producer --dry-run     # print readings to stdout, no Kafka needed
python -m simulators.lab_generator --once 3 --out-dir /tmp/labs
```

Team conventions (branches, reviews, contract changes): see PROJECT_PLAN.md §9.

## Assumptions and simplifications

* Time is compressed 288× (1 simulated day = 5 minutes); all timestamps are UTC.
* 20 synthetic patients, one ward, single Kafka broker (replication 1), no authentication or TLS.
* Synthetic data only. The risk score used downstream is a simplified NEWS2 adaptation for
  demonstration - **not clinically validated**.
* Parquet on a local volume stands in for S3/HDFS.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Containers restart / `OOMKilled` | Give Docker Desktop / WSL2 more memory (`%UserProfile%\.wslconfig`: `memory=8GB`) |
| `exec /scripts/create_topics.sh: no such file` / `\r` errors | Line endings: `git config core.autocrlf false`, re-checkout (`.gitattributes` forces LF for scripts) |
| Simulators keep waiting for Kafka | `docker compose logs kafka kafka-init`; the broker needs ≈ 20-30 s on first start |
| Sim day counter is huge after a break | The epoch file is old: `make reset` (or delete `data/state/sim_epoch`) |
| Grafana ward panels show "relation does not exist" | Expected until B/C create their tables (`sql/init.sql`) |
| Prometheus targets `spark-streaming` / `api` are DOWN | `docker compose ps spark-streaming api`; the API needs Postgres healthy |
| DAG run failed at `wait_for_lab_file` | No lab file for that day (by design with `LAB_FORCE_MISSING_DAYS`); otherwise check `docker compose logs lab-generator` |
| Airflow tasks cannot move files in `data/` (Linux host) | Set `AIRFLOW_UID=$(id -u)` in `.env`, or `sudo chmod -R a+rwX data` |
| Image builds crawl / time out in `pip install` | PyPI's CDN can be very slow on some links: `docker compose build --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple` (any PyPI mirror) |
| Host tools reach the wrong Postgres (`password authentication failed` on `localhost:5432`) | A PostgreSQL installed on Windows owns port 5432, hiding the container's port. Stop that service, or query inside the stack: `docker compose exec postgres psql -U hospital` |
| `/health` returns 503 but the API runs | Stale data (> 60 s) - the speed layer or simulator is down; `/health/live` is the liveness probe |
