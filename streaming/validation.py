"""Parse and validate raw Kafka records (task B3).

Every record gets a ``rejection_reason`` (``NULL`` = valid) instead of being filtered out
silently, so the job can route invalid ones to the DLQ and the counts reconcile:

    input rows = valid + DLQ + dropped duplicates/too-late

Reasons, first match wins (order matters - a malformed record has no vitals to range-check):

    malformed_record        not JSON / no event_id / no patient_id
    bad_timestamp           timestamp missing or not ISO-8601
    future_timestamp        more than 60 s ahead of the processing clock (a bogus future
                            time would drag the event-time watermark forward and make
                            every genuine reading look late)
    missing_field:<name>    null (or wrongly typed) vital or sim_day
    out_of_range:<name>     outside VALID_RANGES (sensor garbage, e.g. HR 0 or 999)
    implausible_bp          diastolic >= systolic
    unknown_patient         patient_id not in the ``patients`` dimension
"""

from __future__ import annotations

from typing import Any

from common.schemas import VITAL_FIELDS, VITALS_SPARK_DDL

# Physiologically possible values (inclusive). Deliberately wider than the simulator's model
# (HR 30-220, SpO2 70-100, ...) so real extremes are kept and only sensor garbage is rejected.
VALID_RANGES: dict[str, tuple[float, float]] = {
    "heart_rate": (20, 250),
    "spo2": (50, 100),
    "systolic_bp": (50, 260),
    "diastolic_bp": (20, 160),
    "temperature": (30.0, 43.0),
}
FUTURE_TOLERANCE_SECONDS = 60


def parse_kafka(kafka_df: Any) -> Any:
    """Kafka rows -> one column per contract field + Kafka coordinates + ``event_time``.

    ``from_json`` in PERMISSIVE mode never fails the query: unparseable JSON or a wrongly
    typed field becomes NULL and is caught by ``rejection_reason`` below.
    """
    from pyspark.sql import functions as F

    parsed = kafka_df.select(
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("value").cast("string").alias("raw_payload"),
    ).withColumn("data", F.from_json("raw_payload", VITALS_SPARK_DDL))
    return parsed.select(
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
        "raw_payload",
        "data.*",
    ).withColumn("event_time", F.to_timestamp("timestamp"))


def rejection_reason_col(known_patient: Any = None) -> Any:
    """Spark ``Column``: first failing rule as a string, or NULL for a valid record.

    ``known_patient`` is a boolean Column (true when the patient join matched); omit it to
    skip the dimension check (tests without a patients table).
    """
    from pyspark.sql import functions as F

    rules: list[tuple[Any, Any]] = [
        (F.col("event_id").isNull() | F.col("patient_id").isNull(), F.lit("malformed_record")),
        (F.col("event_time").isNull(), F.lit("bad_timestamp")),
        (
            F.col("event_time")
            > F.current_timestamp() + F.expr(f"INTERVAL {FUTURE_TOLERANCE_SECONDS} SECONDS"),
            F.lit("future_timestamp"),
        ),
        (F.col("sim_day").isNull(), F.lit("missing_field:sim_day")),
    ]
    rules += [(F.col(v).isNull(), F.lit(f"missing_field:{v}")) for v in VITAL_FIELDS]
    rules += [
        (~F.col(v).between(low, high), F.lit(f"out_of_range:{v}"))
        for v, (low, high) in VALID_RANGES.items()
    ]
    rules.append((F.col("diastolic_bp") >= F.col("systolic_bp"), F.lit("implausible_bp")))
    if known_patient is not None:
        rules.append((~known_patient, F.lit("unknown_patient")))

    expr = None
    for cond, reason in rules:
        expr = F.when(cond, reason) if expr is None else expr.when(cond, reason)
    return expr  # no otherwise(): NULL means valid


def reason_for(record: dict, known_patients: set[str] | None = None) -> str | None:
    """Pure-Python mirror of ``rejection_reason_col`` for one decoded record.

    Used by the tests to state the rules without Spark; the Spark test checks both agree.
    Timestamps are not re-parsed here (``event_time`` is passed in already parsed or None).
    """
    if record.get("event_id") is None or record.get("patient_id") is None:
        return "malformed_record"
    if record.get("event_time") is None:
        return "bad_timestamp"
    if record.get("sim_day") is None:
        return "missing_field:sim_day"
    for v in VITAL_FIELDS:
        if record.get(v) is None:
            return f"missing_field:{v}"
    for v, (low, high) in VALID_RANGES.items():
        if not low <= record[v] <= high:
            return f"out_of_range:{v}"
    if record["diastolic_bp"] >= record["systolic_bp"]:
        return "implausible_bp"
    if known_patients is not None and record["patient_id"] not in known_patients:
        return "unknown_patient"
    return None
