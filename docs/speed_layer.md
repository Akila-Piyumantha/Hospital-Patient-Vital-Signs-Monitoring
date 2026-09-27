# Speed layer & data lake — design, tuning notes and report chapters (Member B)

This file holds Member B's report chapters as drafts (architecture decision, processing layer, storage design)
and the performance/tuning notes (task B11). Code: [streaming/](../streaming), [common/scoring.py](../common/scoring.py),
[sql/02_speed_layer.sql](../sql/02_speed_layer.sql).

---

## 1. Architecture decision: Lambda, not Kappa

> A and C each add one paragraph (marked below): A on ingestion/replay, C on serving/consistency.

**The question the system answers** has two halves with different time scales: *"which patients show concerning
vital-sign trends right now"* needs seconds-level latency over a continuous stream, and *"how do yesterday's lab
results change the risk picture"* depends on a file that arrives once per day and may be late, corrected or missing.
Lambda gives each half the processing model that suits it and merges the results in the serving store.

| Requirement | How Lambda meets it |
|---|---|
| Alerts within seconds | Speed layer: Spark Structured Streaming, 5-s micro-batches, event-time windows |
| Labs arrive as one daily file | Batch layer: Airflow sensor → validate → load; no need to force a file through a log |
| Daily report must be correct | Batch job recomputes the whole day from the immutable Parquet lake, incl. late data the speed layer dropped |
| Replay / backfill any day | Re-run the batch job on `sim_day=N`, independent of Kafka retention |
| One scoring logic | `common/scoring.py` is imported by both layers (thresholds as data → Python and Spark expressions) |

**Rejected alternative — Kappa.** One streaming codebase would process both vitals and labs (labs on a compacted
Kafka topic) and "reprocess" by replaying the log. We rejected it because (1) reprocessing a day needs the full log
anyway — either long Kafka retention or a lake to replay from, i.e. the batch storage Lambda already has; (2) the daily
report is a full recomputation whose natural semantics are "overwrite partition `sim_day=N`", which is simpler and
safer as a batch job than as a stateful streaming job; (3) the lab file is batch by nature and the mandated
orchestrator (Airflow) would have nothing meaningful to orchestrate.

**The price of Lambda, and how we pay it.** Two code paths could drift apart. We mitigate it with one shared scoring
module, and — unusually — we *measure* the drift: the batch layer publishes `speed_batch_discrepancy_ratio`, the
difference between its exact per-day aggregates and the speed layer's windowed ones. The difference is expected to be
non-zero, because the speed layer deliberately trades completeness for latency: readings later than the 1-minute
watermark are left out of the windows (they are still in the lake). The metric makes this trade-off visible instead of hiding it.

*[A: paragraph on ingestion and replay — Kafka retention, partitions keyed by patient, why the lake and not the log is the replay source.]*

*[C: paragraph on serving and consistency — how the API merges real-time tables (`patient_status`, `vitals_window`, `alerts`) with the daily `patient_risk_report`.]*

---

## 2. Processing layer (speed layer)

### 2.1 Pipeline

```
Kafka vitals.raw (3 partitions, key=patient_id)
   │
   ├─ query vitals_readings  (trigger 5 s, checkpoint data/checkpoints/vitals_readings)
   │     from_json(explicit schema) → rejection_reason → ⋈ patients (stream-static)
   │     → ⋈ patient_lab_risk (stream-static, re-read every micro-batch) → MAP, pulse pressure, NEWS sub-scores,
   │       news_score, total = news + lab points, risk_tier
   │     → dropDuplicatesWithinWatermark(event_id)  [watermark on Kafka timestamp, 10 min]
   │     → foreachBatch:  invalid → vitals.dlq + dlq_events
   │                      valid   → Parquet lake · patient_status · reading alerts
   │
   └─ query vitals_windows   (trigger 5 s, checkpoint data/checkpoints/vitals_windows, update mode)
         valid readings → withWatermark(event_time, 1 min) → dropDuplicatesWithinWatermark(event_id)
         → groupBy(patient, window(event_time, 2 min, 30 s)) avg/min/max/count → window NEWS
         → foreachBatch: upsert vitals_window → trend slopes, trend flag, sustained counter → window alerts
```

### 2.2 Design decisions

| Decision | Why |
|---|---|
| **Two queries**, not one | The lake must receive *late* readings (batch layer needs them) while the windows must *drop* them (bounded state). One query cannot have both lateness policies. Each has its own checkpoint, so each recovers independently. |
| Validation marks, never filters | A `rejection_reason` column (first failing rule) lets the same micro-batch route rows to the DLQ, and makes counts reconcile: `input = valid + dlq + dropped` (`vitals_input_total`, `vitals_valid_total`, `vitals_dlq_total`, `vitals_dropped_total`). |
| Readings dedupe watermark on the **Kafka** timestamp | A re-sent message arrives seconds after the original, so 10 min on broker time removes duplicates, while an event 90 s late (old event time) still passes. An event-time watermark there would drop exactly the late data the lake must keep. |
| Future timestamps (> 60 s ahead) rejected | A single bogus future time would push the event-time watermark forward and make every genuine reading "late". |
| Windows 2 min / slide 30 s, watermark 1 min | Sim clock compresses a day into 5 minutes, so the plan's "5-minute trend" is scaled to 2-min windows (not clinically calibrated). A 30-s slide gives the ward view a fresh point every 30 s; 1 min of lateness covers normal network jitter, while the simulator's 20–90 s "late" faults show the trade-off. |
| Update output mode | A window's aggregates appear after the first trigger and refine every 5 s; append mode would delay everything until the watermark passes (≈ 3 min). Upserts make update mode idempotent. |
| Stream-static joins, static side not cached | Spark re-plans the static side each micro-batch, so a new `patient_lab_risk` row is used within one trigger — the lab feedback loop needs no restart and no extra stream. |
| Trend/sustained logic in the driver over stored windows | Chained stateful aggregations are limited in Spark 3.5. The inputs are ≤ 8 windows per patient, bounded by ward size (not by message rate), and the logic is pure Python and unit-tested. |
| Alerts in event time, deterministic ids | `uuid5(patient, reason, opened_at)` + a unique index on open alerts: replaying a batch re-derives the same alerts, and the inserts are no-ops. |

### 2.3 Scoring (shared with the batch layer)

NEWS2-style sub-scores for HR, SpO₂, systolic BP and temperature (bands in `common/scoring.py`, tested at every edge),
`total = news_score + lab_risk_points` (lab points capped at 4) → LOW 0–2 · MEDIUM 3–4 · HIGH 5–6 · CRITICAL ≥ 7;
any single vital scoring 3 ⇒ at least MEDIUM. **Simplified and not clinically validated** (no respiration rate or consciousness level).

### 2.4 Alerts

| Reason code | Rule | Severity |
|---|---|---|
| `HR_CRITICAL` / `SPO2_CRITICAL` / `SBP_CRITICAL` / `TEMP_CRITICAL` | any reading in the batch scores 3 for that vital | HIGH |
| `TOTAL_SCORE_HIGH` | news + lab points ≥ 5 | HIGH (CRITICAL if ≥ 7) |
| `SUSTAINED_ABNORMAL` | window NEWS ≥ 3 in ≥ 3 consecutive windows | HIGH |
| `WORSENING_TREND` | over the last 5 windows: HR ≥ +4 bpm/min, SpO₂ ≤ −1 %/min, SBP ≤ −5 mmHg/min, or window NEWS strictly rising over 3 windows | MEDIUM |

Lifecycle: open → touched while the condition holds → resolved when the patient's newest data no longer meets it;
60-s cooldown after resolution per patient+reason. A transient spike opens and resolves an alert in the same batch
(history kept, the ward's "open alerts" list stays calm).

### 2.5 Fault handling

| Fault (simulator) | Where it is handled | Evidence |
|---|---|---|
| null vital | `missing_field:<vital>` → DLQ | `vitals_dlq_total{reason}`, `dlq_events` |
| garbage value (HR 0/999, SpO₂ 150 …) | `out_of_range:<vital>` → DLQ | same |
| duplicate `event_id` | `dropDuplicatesWithinWatermark` in both queries | `vitals_dropped_total`, no double-counted `n_readings` |
| late event (20–90 s) | lake: kept · windows: kept if ≤ 1 min, else dropped | `spark_rows_dropped_by_watermark_total`, discrepancy metric |
| sensor dropout | `pipeline_last_event_age_seconds`, patient's `last_reading_at` ages | Grafana freshness panel |
| Spark crash / restart | checkpoints + idempotent sinks | restart test below |

---

## 3. Storage design (speed-layer side)

| Store | Content | Key / layout | Why |
|---|---|---|---|
| Parquet lake | every valid reading + `event_time`, MAP, pulse pressure, NEWS, Kafka coordinates, `ingested_at` | `data/lake/vitals/sim_day=N/part-<checkpoint>-b<batch>-NNN.parquet` | Immutable master dataset of the Lambda batch layer; columnar and partition-pruned by day; files named after checkpoint + micro-batch → a replay overwrites its own files instead of duplicating, a new checkpoint never overwrites old ones |
| `vitals_window` | per patient × 2-min window: avg/min/max of 5 vitals, count, window NEWS, trend slopes | PK `(patient_id, window_start)` | Upsert target of update-mode output; time-series charts; trend inputs |
| `patient_status` | one row per patient: latest vitals, scores, lab points, tier, trend flag | PK `patient_id`; guarded upsert (`last_reading_at` never goes back) | "Right now" ward view in O(patients) |
| `alerts` | alert lifecycle | PK `alert_id` (uuid5), unique open alert per patient+reason | Replay-safe, queryable history |
| `dlq_events` | rejected records with reason and raw payload | unique `(kafka_partition, kafka_offset)` | Auditable data quality; replay-safe |

PostgreSQL rather than Cassandra: small, frequently *updated* aggregates (upserts), ad-hoc SQL joins with
`patients` and the batch tables, and direct Grafana/FastAPI access. The write load is tiny (≈ 20 patient rows +
≈ 100 window rows every 5 s).

At production scale: lake on S3/HDFS with a table format (Delta/Iceberg) for atomic multi-file commits and
compaction of the small per-batch files; windows and status in a time-series store or partitioned Postgres tables
with retention.

---

## 4. Performance and tuning notes (B11)

| Setting | Value | Reason |
|---|---|---|
| `spark.sql.shuffle.partitions` | 3 (= Kafka partitions) | Default 200 gives 200 state-store tasks per stateful operator per batch — seconds of pure overhead at ~50 rows/batch. Fixed once a checkpoint exists (change → delete checkpoint). |
| Trigger | 5 s | ~50 readings per batch; batches finish well within the trigger, so no backlog builds up. Alert latency ≤ trigger + batch duration. |
| `maxOffsetsPerTrigger` | 5000 | Bounds the first batch after downtime (catch-up happens in slices, not one huge batch). |
| Master / memory | `local[2]`, driver 1 GB, container limit 2 GB | Laptop-sized; Kafka partitions (3) bound the useful parallelism anyway. |
| State store | default (HDFS-backed, in memory) | State is small (dedupe keys ≤ 10 min × 10 msg/s ≈ 6 000 keys, open windows ≈ 20 × 5). RocksDB state store for larger state. |
| Broadcast static joins | `patients`, `patient_lab_risk` (20 rows) | No shuffle for enrichment. |
| Spark actions per micro-batch | readings: 2 (one aggregation + Parquet) · windows: 1 | Measured ~1 s fixed cost per Spark job in local mode, regardless of size. The readings sink first ran ~5 jobs (counts, DLQ→Kafka, DLQ→Postgres, Parquet, summaries); one `groupBy(patient_id)` now returns summaries, valid count and rejected records together. Postgres/DLQ writes run on the driver over rows bounded by ward size / `maxOffsetsPerTrigger` (~20 ms per batch). |
| Checkpoint location | named Docker volume | On Docker Desktop for Windows a bind-mounted checkpoint + state store roughly doubled the batch time (below). Lake stays on `./data` (contract with the batch layer). |

### 4.1 Measurements (2026-09-27)

Setup: Docker Desktop on Windows 11 (8 vCPU, 8 GB), 20 patients × 1 reading/2 s (~10 msg/s), `FAULT_PROFILE=low`,
`local[2]`, trigger 5 s. Verification stack: Kafka 3.7 (KRaft), Postgres 15, A's simulators, this job.

| Configuration | readings batch (mean / p95) | windows batch | lake write | freshness¹ |
|---|---|---|---|---|
| Initial sink (5 Spark jobs), checkpoints + lake on Windows bind mount | 7.7 s / 9.2 s | 5.4 s | 2–4.5 s | ~12–16 s |
| Same, `local[4]` | 8.6 s / 10.6 s | 4.8 s | — | ~10 s |
| 2-job sink, everything on bind mount | 7.9 s / 10.4 s | 4.8 s | 2–4.5 s | ~12 s |
| 2-job sink, checkpoints + lake on named volume | 3.8 s / 4.7 s | 2.9 s | 0.4 s | ~6.6 s |
| **Shipped:** 2-job sink, checkpoints on volume, lake on bind mount | **3.9 s / 5.4 s**² | **2.4 s** | 0.7 s | **~6 s** |

¹ `pipeline_last_event_age_seconds` = now − newest event time processed. ² while also catching up a backlog (272 rows/batch).

Findings: (1) the job is not CPU-bound — more cores did not help (`local[4]`); (2) the dominant cost was filesystem
latency of the Windows bind mount for checkpoints/state store and the lake, not the Spark logic; on Linux or with the repo
inside WSL2 (plan §9) bind mounts are native and the difference disappears; (3) memory: ~1.6 GB of the 2 GB limit
(driver heap 1 GB + Python workers).

### 4.2 Correctness checks on the live stack

| Check | Result |
|---|---|
| Count reconciliation `input = valid + DLQ + dropped` | 21 255 = 20 633 + 419 + 203 ✔ |
| Lake duplicates | 20 577 rows = 20 577 distinct `event_id` ✔ |
| Speed vs. lake for the same window | per-window `n_readings` equal to the lake's distinct ids ✔ |
| **Kill -9 during batch 165** (offsets written, commit missing) → restart | batch 165 replayed first (same id), still exactly 1 lake file for it; 0 duplicate windows; 170 alerts = 170 distinct ids; 0 double-open alerts; 413 DLQ rows = 413 distinct offsets ✔ |
| Lab feedback loop (B8) | lab file of day 9 loaded → picked up **10 s later without restart**; occult patients P005 (NEWS 0 + 4 lab points) and P012 LOW → MEDIUM; P008, P014 LOW → MEDIUM |
| Ground truth (simulator's hidden story) | deteriorating P001/P004/P008/P009: 24.5 alerts per patient, each got SUSTAINED_ABNORMAL + TOTAL_SCORE_HIGH + WORSENING_TREND; stable patients: 9.2 (mostly single-vital alerts on the simulator's 1 % random spikes) |
| Consumer lag visible to kafka-exporter | group `spark-speed-layer` committed on all 3 partitions, lag 9–25 messages ✔ |

### 4.3 Known limitations (for the report)

* Single-vital alerts fire on every random spike (contract: "single vital score 3 ⇒ alert"); a production rule would
  require persistence (e.g. 2 of 3 readings) — a one-line change in `alerts.reading_signals`.
* `WORSENING_TREND` has false positives on stable patients (noise + spikes in 2-min averages); thresholds are
  data in `common/scoring.py` (`TREND_SLOPE_THRESHOLDS`).
* The `vitals.dlq` topic is at-least-once (a replayed batch may re-publish); `dlq_events` is exactly-once.
* Late data beyond the 1-min watermark is missing from `vitals_window` by design; it is in the lake and the batch
  layer's discrepancy metric quantifies it.
* A fresh checkpoint with `STREAM_STARTING_OFFSETS=earliest` re-reads the topic; the lake then holds those readings
  twice (different file names, never overwritten) — the batch layer dedupes on `event_id`.
