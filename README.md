# Hospital Patient Vital Signs Monitoring

End-to-end **Lambda-architecture** data pipeline (EC8203 Applied Big Data Engineering mini-project,
use case 2): near-real-time bedside vital signs are correlated with the pathology lab's once-a-day
results to answer

> *Which patients show concerning vital-sign trends right now, and how do yesterday's lab results
> change the risk picture for those patients going forward?*

Full design, task split and contracts: [PROJECT_PLAN.md](PROJECT_PLAN.md) · progress: [TASK_BOARD.md](TASK_BOARD.md) ·
contract changes: [docs/CHANGELOG.md](docs/CHANGELOG.md)

> **Status:** platform, ingestion and observability (Member A) are implemented. Spark speed layer (B) and
> Airflow / API / reports (C) plug in through `compose/spark.yml`, `compose/airflow.yml`, `compose/api.yml`.

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

### Soak run (measured)

`python scripts/soak_monitor.py --minutes 30` samples the stack every 30 s into `data/soak/soak.csv` and
writes `data/soak/summary.json`. Result of the platform layer alone (simulators + Kafka + Postgres +
observability, `FAULT_PROFILE=low`), 2026-09-25:

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
  The ward dashboard's panels fill in once B/C's tables exist.

## Repository layout

```
common/            config, simulated clock, JSON logging, data-contract models   (A)
simulators/        vitals producer, lab generator, patient model, faults, seeding (A)
kafka/             topic creation script                                          (A)
sql/               00_ airflow DB · 01_ patients table · init.sql (C: rest of schema)
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
| Prometheus targets `spark-streaming` / `api` are DOWN | Expected until B/C add their services |
