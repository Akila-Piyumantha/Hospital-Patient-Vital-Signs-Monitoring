"""Speed layer entry point: ``python -m streaming.stream_job`` (owner: Member B).

Two streaming queries read ``vitals.raw`` independently (each has its own checkpoint):

``vitals_readings``  every record, no event-time lateness limit
    parse -> validate -> enrich (patients + lab risk, stream-static joins) -> dedupe by
    ``event_id`` (watermark on the *Kafka* timestamp, so late sensor data is never dropped)
    -> foreachBatch:
       invalid -> ``vitals.dlq`` topic + ``dlq_events``
       valid   -> Parquet lake ``sim_day=N`` (batch layer's source of truth)
               -> ``patient_status`` (latest vitals, NEWS, lab points, risk tier)
               -> reading-level alerts (single vital scores 3, total score >= 5)

``vitals_windows``   valid readings, event-time semantics
    dedupe within 1-min watermark -> 2-min/30-s sliding windows (update mode)
    -> foreachBatch: upsert ``vitals_window`` -> trend slopes, trend flag, sustained
       counter -> window-level alerts (sustained abnormal, worsening trend)

Metrics on :8003 (``metrics_listener``), JSON logs on stdout, Spark UI on :4040.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prometheus_client import start_http_server

from common.logging_setup import configure_logging, get_logger
from streaming import alerts, metrics_listener, sinks
from streaming.config import StreamSettings
from streaming.enrichment import build_readings, jdbc_reader, static_lab_risk, static_patients
from streaming.windows import WindowPoint, analyse_patient_windows, windowed_vitals

READINGS_QUERY = "vitals_readings"
WINDOWS_QUERY = "vitals_windows"

log = get_logger("streaming.job", "processing")
store_log = get_logger("streaming.sinks", "storage")


# ------------------------------------------------------------------- Spark plumbing
def build_spark(settings: StreamSettings) -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master(settings.spark_master)
        .appName("vitals-speed-layer")
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", _env("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def _env(name: str, default: str) -> str:
    from common.config import env_str

    return env_str(name, default)


def kafka_stream(spark: Any, settings: StreamSettings) -> Any:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.base.kafka_bootstrap_servers)
        .option("subscribe", settings.base.vitals_topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")  # retention may delete offsets while we are down
        .load()
    )


def _reader(spark: Any, settings: StreamSettings) -> Any:
    b = settings.base
    return jdbc_reader(spark, settings.jdbc_url, b.postgres_user, b.postgres_password)


# ------------------------------------------------------------- readings micro-batches
def batch_summaries(batch_df: Any) -> Any:
    """One row per patient_id with everything the driver needs from a readings micro-batch.

    * ``n_valid`` and ``rejected`` (the invalid records, for the DLQ),
    * over valid readings only: the newest reading (``latest``), and per alert rule the first
      event time it fired and the worst value.

    A single aggregation = a single Spark job per batch (plus the Parquet write): at ~1 s of
    fixed cost per job in local mode, the number of actions dominates the batch duration.
    """
    from pyspark.sql import functions as F

    from common import scoring

    valid = F.col("rejection_reason").isNull()

    def when_valid(cond: Any, value: Any) -> Any:
        return F.when(valid & cond, value)

    latest_fields = [
        "event_time",
        *[c for c in sinks.STATUS_COLUMNS if c not in ("patient_id", "last_reading_at")],
        *[f"{v}_score" for v in scoring.SCORED_VITALS],
    ]
    rejected = F.struct(
        "patient_id", "rejection_reason", "raw_payload", "kafka_partition", "kafka_offset"
    )
    aggs = [
        F.count(F.when(valid, 1)).alias("n_valid"),
        F.collect_list(F.when(~valid, rejected)).alias("rejected"),
        F.max_by(F.struct(*latest_fields), F.when(valid, F.col("event_time"))).alias("latest"),
        F.max(F.when(valid, F.col("total_score"))).alias("total_max"),
        F.min(
            when_valid(F.col("total_score") >= scoring.ALERT_TOTAL_SCORE, F.col("event_time"))
        ).alias("total_first_high_at"),
    ]
    for v in scoring.SCORED_VITALS:
        trigger_time = when_valid(
            F.col(f"{v}_score") >= scoring.CRITICAL_VITAL_SCORE, F.col("event_time")
        )
        aggs += [
            F.min(trigger_time).alias(f"{v}_first3_at"),
            F.min_by(v, trigger_time).alias(f"{v}_worst"),
        ]
    return batch_df.groupBy("patient_id").agg(*aggs)


def checkpoint_tag(checkpoint: Path) -> str:
    """Short id of a query's checkpoint (from its ``metadata`` file, written before batch 0)."""
    meta = json.loads((checkpoint / "metadata").read_text(encoding="utf-8"))
    return meta["id"].replace("-", "")[:8]


class ReadingsSink:
    def __init__(self, settings: StreamSettings) -> None:
        self.s = settings
        self._producer = None
        self._run_tag: str | None = None

    def _dlq_producer(self) -> Any:
        if self._producer is None:
            from confluent_kafka import Producer

            self._producer = Producer(
                {
                    "bootstrap.servers": self.s.base.kafka_bootstrap_servers,
                    "enable.idempotence": True,
                    "acks": "all",
                }
            )
        return self._producer

    def __call__(self, batch_df: Any, batch_id: int) -> None:
        from pyspark.sql import functions as F

        started = time.time()
        batch_df.persist()
        try:
            rows = [r.asDict(recursive=True) for r in batch_summaries(batch_df).collect()]
            t_aggregate = time.time()
            rejected = [rec for r in rows for rec in r["rejected"]]
            summaries = [r for r in rows if r["n_valid"] and r["patient_id"] is not None]
            valid_count = sum(r["n_valid"] for r in rows)
            dlq_counts: dict[str, int] = {}
            for rec in rejected:
                reason = rec["rejection_reason"]
                dlq_counts[reason] = dlq_counts.get(reason, 0) + 1

            files = 0
            if valid_count:
                lake_df = (
                    batch_df.filter(F.col("rejection_reason").isNull())
                    .select(*sinks.LAKE_COLUMNS)
                    .withColumn("ingested_at", F.current_timestamp())
                )
                if self._run_tag is None:
                    self._run_tag = checkpoint_tag(Path(self.s.checkpoint_dir) / READINGS_QUERY)
                files = sinks.write_lake(
                    lake_df, self.s.lake_dir, self.s.lake_staging_dir, batch_id, self._run_tag
                )
            t_lake = time.time()
            self._update_patients(summaries, rejected)

            metrics_listener.record_readings_batch(batch_id, valid_count, dlq_counts)
            store_log.info(
                "readings_batch_written",
                query=READINGS_QUERY,
                batch_id=batch_id,
                rows_valid=valid_count,
                rows_dlq=len(rejected),
                dlq_reasons=dlq_counts,
                lake_files=files,
                duration_ms=round((time.time() - started) * 1000),
                step_ms={
                    "aggregate": round((t_aggregate - started) * 1000),
                    "lake": round((t_lake - t_aggregate) * 1000),
                    "postgres_dlq": round((time.time() - t_lake) * 1000),
                },
            )
        finally:
            batch_df.unpersist()

    def _update_patients(self, summaries: list[dict], rejected: list[dict]) -> None:
        ids = [s["patient_id"] for s in summaries]
        latest_rows = []
        with sinks.connect(self.s.pg) as conn, conn.cursor() as cur:
            if rejected:
                sinks.write_dlq(cur, self._dlq_producer(), self.s.base.dlq_topic, rejected)
            before = sinks.fetch_status(cur, ids)
            open_alerts, last_resolved = sinks.fetch_alert_state(cur, ids, alerts.READING_REASONS)
            actions = alerts.Actions()
            newest_seen = None
            for summ in summaries:
                pid = summ["patient_id"]
                latest = summ["latest"]
                event_time = sinks.utc(latest["event_time"])
                newest_seen = max(newest_seen or event_time, event_time)
                prev = before.get(pid, {})
                prev_at = prev.get("last_reading_at")
                is_newest = prev_at is None or event_time >= prev_at

                row = {k: latest.get(k) for k in sinks.STATUS_COLUMNS}
                row.update(patient_id=pid, last_reading_at=event_time)
                latest_rows.append(row)
                if is_newest:
                    self._log_changes(pid, prev, row)

                summ = {k: sinks.utc(v) for k, v in summ.items()}
                summ["is_newest"] = is_newest
                summ["total_latest"] = latest["total_score"]
                for v in ("heart_rate", "spo2", "systolic_bp", "temperature"):
                    summ[f"{v}_score_latest"] = latest[f"{v}_score"]
                alerts.evaluate(
                    pid,
                    alerts.reading_signals(summ),
                    event_time if is_newest else prev_at,
                    {r: a for (p, r), a in open_alerts.items() if p == pid},
                    {r: t for (p, r), t in last_resolved.items() if p == pid},
                    actions=actions,
                )
            sinks.upsert_status(cur, latest_rows)
            inserted = sinks.apply_alert_actions(cur, actions)
            cur.execute(
                "SELECT count(DISTINCT patient_id) FROM patient_status WHERE lab_risk_points > 0"
            )
            metrics_listener.LAB_RISK_PATIENTS.set(cur.fetchone()[0])
        _record_alerts(inserted, actions)
        metrics_listener.observe_event_time(newest_seen)

    @staticmethod
    def _log_changes(pid: str, prev: dict, row: dict) -> None:
        if prev and prev.get("lab_risk_points") != row["lab_risk_points"]:
            log.info(
                "lab_risk_applied",
                patient_id=pid,
                lab_points_before=prev.get("lab_risk_points"),
                lab_points_after=row["lab_risk_points"],
                lab_as_of_sim_day=row["lab_as_of_sim_day"],
                tier_before=prev.get("risk_tier"),
                tier_after=row["risk_tier"],
            )
        if prev.get("risk_tier") != row["risk_tier"]:
            log.info(
                "risk_tier_changed",
                patient_id=pid,
                tier_before=prev.get("risk_tier"),
                tier_after=row["risk_tier"],
                news_score=row["news_score"],
                lab_risk_points=row["lab_risk_points"],
                total_score=row["total_score"],
            )


# --------------------------------------------------------------- windows micro-batches
class WindowsSink:
    def __init__(self, settings: StreamSettings) -> None:
        self.s = settings

    def __call__(self, batch_df: Any, batch_id: int) -> None:
        started = time.time()
        # update mode emits only changed windows: <= patients x (window / slide) rows
        rows = batch_df.select(*sinks.WINDOW_COLUMNS).collect()
        if not rows:
            return
        flagged, ids = self._analyse(rows)
        store_log.info(
            "windows_batch_written",
            query=WINDOWS_QUERY,
            batch_id=batch_id,
            windows=len(rows),
            patients=len(ids),
            trend_flagged=flagged,
            duration_ms=round((time.time() - started) * 1000),
        )

    def _analyse(self, rows: list[Any]) -> tuple[int, list[str]]:
        from common import scoring

        ids = sorted({r["patient_id"] for r in rows})
        now = datetime.now(UTC)
        with sinks.connect(self.s.pg) as conn, conn.cursor() as cur:
            sinks.upsert_rows(cur, sinks.WINDOW_UPSERT_SQL, sinks.WINDOW_COLUMNS, rows)
            recent = sinks.fetch_recent_windows(
                cur, ids, self.s.min_window_readings, scoring.TREND_WINDOWS + 3
            )
            results = {}
            for pid, rows in recent.items():
                res = analyse_patient_windows([WindowPoint(*r) for r in rows])
                if res is not None:
                    results[pid] = res
            sinks.update_trends(cur, results)

            open_alerts, last_resolved = sinks.fetch_alert_state(cur, ids, alerts.WINDOW_REASONS)
            actions = alerts.Actions()
            for pid, res in results.items():
                # a window's end lies in the future while it fills; never date an alert ahead
                at = min(res.latest.window_end, now)
                alerts.evaluate(
                    pid,
                    alerts.window_signals(res.sustained_windows, res.flags, at),
                    at,
                    {r: a for (p, r), a in open_alerts.items() if p == pid},
                    {r: t for (p, r), t in last_resolved.items() if p == pid},
                    actions=actions,
                )
            inserted = sinks.apply_alert_actions(cur, actions)
        _record_alerts(inserted, actions)
        return sum(1 for r in results.values() if r.flags), ids


def _record_alerts(inserted: list[dict], actions: alerts.Actions) -> None:
    for a in inserted:
        metrics_listener.ALERTS_OPENED.labels(severity=a["severity"]).inc()
        log.info(
            "alert_opened",
            patient_id=a["patient_id"],
            reason_code=a["reason_code"],
            severity=a["severity"],
            value=a["value"],
            threshold=a["threshold"],
            opened_at=a["opened_at"],
        )
    if actions.resolves:
        metrics_listener.ALERTS_RESOLVED.inc(len(actions.resolves))
    if actions.suppressed:
        metrics_listener.ALERTS_SUPPRESSED.inc(actions.suppressed)


# ------------------------------------------------------------------------------ main
def wait_for_postgres(settings: StreamSettings, attempts: int = 60) -> None:
    for attempt in range(1, attempts + 1):
        try:
            sinks.ensure_schema(settings.pg)
            log.info(
                "schema_ready", tables=["vitals_window", "patient_status", "alerts", "dlq_events"]
            )
            return
        except Exception as exc:
            log.warning("postgres_not_ready", attempt=attempt, error=repr(exc))
            time.sleep(2)
    raise RuntimeError("postgres unreachable")


def start_queries(spark: Any, settings: StreamSettings) -> list[Any]:
    from pyspark.sql import functions as F

    cp = Path(settings.checkpoint_dir)
    trigger = {"processingTime": settings.trigger_interval}

    readings = build_readings(
        kafka_stream(spark, settings),
        static_patients(_reader(spark, settings)),
        static_lab_risk(_reader(spark, settings)),
    )
    # Dedupe horizon on the Kafka timestamp: a duplicate is re-sent within seconds, while a
    # late sensor reading (old event time) is still accepted and lands in the lake.
    readings = (
        readings.withColumn(
            "dedup_key",
            F.coalesce(
                F.col("event_id"),
                F.concat_ws("-", F.lit("kafka"), F.col("kafka_partition"), F.col("kafka_offset")),
            ),
        )
        .withWatermark("kafka_ts", settings.lake_watermark)
        .dropDuplicatesWithinWatermark(["dedup_key"])
    )
    q_readings = (
        readings.writeStream.queryName(READINGS_QUERY)
        .foreachBatch(ReadingsSink(settings))
        .option("checkpointLocation", str(cp / READINGS_QUERY))
        .trigger(**trigger)
        .start()
    )

    valid = build_readings(
        kafka_stream(spark, settings), static_patients(_reader(spark, settings)), None
    ).filter(F.col("rejection_reason").isNull())
    windows = windowed_vitals(
        valid, settings.window_duration, settings.window_slide, settings.watermark
    )
    q_windows = (
        windows.writeStream.queryName(WINDOWS_QUERY)
        .outputMode("update")
        .foreachBatch(WindowsSink(settings))
        .option("checkpointLocation", str(cp / WINDOWS_QUERY))
        .trigger(**trigger)
        .start()
    )
    return [q_readings, q_windows]


def main() -> int:
    settings = StreamSettings.from_env()
    run_id = configure_logging(
        "spark-streaming", settings.base.log_level, default_stage="processing"
    )
    log.info(
        "starting",
        run_id=run_id,
        topic=settings.base.vitals_topic,
        window=settings.window_duration,
        slide=settings.window_slide,
        watermark=settings.watermark,
        trigger=settings.trigger_interval,
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)  # py4j logs every callback at INFO
    start_http_server(settings.metrics_port)
    wait_for_postgres(settings)

    spark = build_spark(settings)
    spark.sparkContext.setLogLevel("WARN")
    committer = metrics_listener.OffsetCommitter(
        settings.base.kafka_bootstrap_servers, settings.consumer_group
    )
    spark.streams.addListener(metrics_listener.make_listener(READINGS_QUERY, committer))
    queries = start_queries(spark, settings)

    def shutdown(signum: int, _frame: Any) -> None:
        log.info("shutdown_requested", signal=signum)
        for q in queries:
            q.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        spark.streams.awaitAnyTermination()
    finally:
        failed = [q for q in queries if q.exception() is not None]
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
    if failed:
        log.error("query_failed", query=failed[0].name, error=str(failed[0].exception()))
        return 1
    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
