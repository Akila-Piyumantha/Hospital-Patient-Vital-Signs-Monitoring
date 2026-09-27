"""Prometheus metrics + per-batch structured logs of the streaming job (task B9).

Served on ``METRICS_PORT`` (8003), scraped as job ``spark-streaming``. Names follow
contract 4.7 so Member A's alert rules and dashboards work unchanged:

    spark_input_rows_per_sec{query}      spark_batch_duration_seconds{query}
    spark_watermark_lag_seconds{query}   vitals_valid_total   vitals_dlq_total{reason}
    alerts_opened_total{severity}        pipeline_last_event_age_seconds

plus ``vitals_input_total`` / ``vitals_dropped_total`` (so input = valid + dlq + dropped
can be checked in Prometheus), ``spark_query_active{query}`` and ``alerts_suppressed_total``.

Consumer lag: Spark tracks Kafka offsets in its checkpoint and never commits them to a
consumer group, so ``kafka_consumergroup_lag`` (kafka-exporter, alert ``ConsumerLagHigh``)
would stay empty. After every readings micro-batch the listener commits the batch's end
offsets to ``STREAM_CONSUMER_GROUP`` - purely for monitoring; recovery still uses the
checkpoint.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from typing import Any

from prometheus_client import Counter, Gauge

from common.logging_setup import get_logger

log = get_logger("streaming.metrics", "processing")

INPUT_RATE = Gauge("spark_input_rows_per_sec", "Kafka rows/s read by the query", ["query"])
PROCESSED_RATE = Gauge("spark_processed_rows_per_sec", "Rows/s processed by the query", ["query"])
BATCH_DURATION = Gauge(
    "spark_batch_duration_seconds", "Duration of the last micro-batch", ["query"]
)
WATERMARK_LAG = Gauge(
    "spark_watermark_lag_seconds", "Wall clock minus the query's event-time watermark", ["query"]
)
STATE_ROWS = Gauge("spark_state_rows", "Rows held in the query's state stores", ["query"])
LATE_ROWS = Counter(
    "spark_rows_dropped_by_watermark_total",
    "Rows dropped as too late by stateful operators",
    ["query"],
)
QUERY_ACTIVE = Gauge("spark_query_active", "1 while the streaming query runs", ["query"])
BATCHES = Counter("spark_batches_total", "Completed micro-batches", ["query"])

VITALS_INPUT = Counter("vitals_input_total", "Kafka records read by the readings query")
VITALS_VALID = Counter("vitals_valid_total", "Readings that passed validation (after dedupe)")
VITALS_DLQ = Counter("vitals_dlq_total", "Readings rejected to the DLQ", ["reason"])
VITALS_DROPPED = Counter(
    "vitals_dropped_total", "Duplicates and too-late records dropped before validation"
)
ALERTS_OPENED = Counter("alerts_opened_total", "Patient alerts opened", ["severity"])
ALERTS_RESOLVED = Counter("alerts_resolved_total", "Patient alerts resolved")
ALERTS_SUPPRESSED = Counter("alerts_suppressed_total", "Alerts suppressed by the cooldown")
LAST_EVENT_AGE = Gauge(
    "pipeline_last_event_age_seconds", "Seconds since the newest reading processed (event time)"
)
LAB_RISK_PATIENTS = Gauge(
    "speed_layer_patients_with_lab_risk", "Patients whose current risk includes lab points"
)

_started = time.time()
_last_event_ts: float | None = None
_lock = threading.Lock()
_batch_counts: dict[int, int] = {}  # readings batch_id -> valid + dlq rows written


def _event_age() -> float:
    return time.time() - (_last_event_ts if _last_event_ts is not None else _started)


LAST_EVENT_AGE.set_function(_event_age)


def observe_event_time(latest: datetime | None) -> None:
    global _last_event_ts
    if latest is None:
        return
    ts = latest.timestamp()
    with _lock:
        if _last_event_ts is None or ts > _last_event_ts:
            _last_event_ts = ts


def record_readings_batch(batch_id: int, valid: int, dlq_by_reason: dict[str, int]) -> None:
    VITALS_VALID.inc(valid)
    for reason, count in dlq_by_reason.items():
        VITALS_DLQ.labels(reason=reason).inc(count)
    with _lock:
        _batch_counts[batch_id] = valid + sum(dlq_by_reason.values())


def _parse_ts(text: str | None) -> float | None:
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def _end_offsets(progress: Any) -> dict[str, dict[str, int]]:
    offsets: dict[str, dict[str, int]] = {}
    for source in progress.sources:
        if source.endOffset and source.endOffset.strip().startswith("{"):
            for topic, parts in json.loads(source.endOffset).items():
                offsets.setdefault(topic, {}).update({p: int(o) for p, o in parts.items()})
    return offsets


class OffsetCommitter:
    """Commits end offsets to a consumer group (monitoring only; see module docstring)."""

    def __init__(self, bootstrap: str, group: str) -> None:
        self.group = group
        self._admin = None
        self._bootstrap = bootstrap

    def commit(self, offsets: dict[str, dict[str, int]]) -> None:
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
        from confluent_kafka.admin import AdminClient

        if self._admin is None:
            self._admin = AdminClient({"bootstrap.servers": self._bootstrap})
        partitions = [
            TopicPartition(topic, int(p), off)
            for topic, parts in offsets.items()
            for p, off in parts.items()
        ]
        if not partitions:
            return
        futures = self._admin.alter_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(self.group, partitions)]
        )
        for future in futures.values():
            future.result(timeout=10)


def make_listener(readings_query: str, committer: OffsetCommitter | None) -> Any:
    """Build the ``StreamingQueryListener`` (class defined lazily: needs pyspark)."""
    from pyspark.sql.streaming import StreamingQueryListener

    names: dict[str, str] = {}  # query id -> name (terminated events carry no name)

    class MetricsListener(StreamingQueryListener):
        def onQueryStarted(self, event: Any) -> None:
            names[str(event.id)] = event.name
            QUERY_ACTIVE.labels(query=event.name).set(1)
            log.info("query_started", query=event.name, query_id=str(event.id))

        def onQueryProgress(self, event: Any) -> None:
            p = event.progress
            name = p.name
            try:
                INPUT_RATE.labels(query=name).set(p.inputRowsPerSecond or 0.0)
                PROCESSED_RATE.labels(query=name).set(p.processedRowsPerSecond or 0.0)
                duration_ms = (p.durationMs or {}).get("triggerExecution", 0)
                BATCH_DURATION.labels(query=name).set(duration_ms / 1000.0)
                BATCHES.labels(query=name).inc()
                watermark = _parse_ts((p.eventTime or {}).get("watermark"))
                if watermark and watermark > 0:
                    WATERMARK_LAG.labels(query=name).set(max(0.0, time.time() - watermark))
                state_rows = sum(op.numRowsTotal for op in p.stateOperators)
                late = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
                STATE_ROWS.labels(query=name).set(state_rows)
                if late:
                    LATE_ROWS.labels(query=name).inc(late)

                dropped = None
                if name == readings_query:
                    VITALS_INPUT.inc(p.numInputRows)
                    with _lock:
                        written = _batch_counts.pop(p.batchId, None)
                    if written is not None:
                        dropped = max(0, p.numInputRows - written)
                        VITALS_DROPPED.inc(dropped)
                    if committer is not None and p.numInputRows > 0:
                        committer.commit(_end_offsets(p))

                log.info(
                    "micro_batch_progress",
                    query=name,
                    batch_id=p.batchId,
                    rows=p.numInputRows,
                    rows_dropped=dropped,
                    input_rows_per_sec=round(p.inputRowsPerSecond or 0.0, 2),
                    duration_ms=duration_ms,
                    duration_breakdown_ms=p.durationMs,
                    watermark=(p.eventTime or {}).get("watermark"),
                    state_rows=state_rows,
                    late_rows=late,
                )
            except Exception as exc:  # a metrics hiccup must never stop the query
                log.warning("metrics_listener_error", query=name, error=repr(exc))

        def onQueryIdle(self, event: Any) -> None:  # Spark >= 3.5
            pass

        def onQueryTerminated(self, event: Any) -> None:
            name = names.get(str(event.id), "unknown")
            QUERY_ACTIVE.labels(query=name).set(0)
            log.error("query_terminated", query=name, query_id=str(event.id), error=event.exception)

    return MetricsListener()
