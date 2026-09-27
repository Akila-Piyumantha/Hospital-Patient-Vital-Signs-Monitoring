"""Local-mode Spark tests of the speed layer on tests/fixtures/vitals_fixture.jsonl.

Skipped when pyspark (or Java) is unavailable; the CI job ``spark-tests`` runs them.
The fixture holds 10 Kafka values: 4 valid P001 readings (one a duplicate), P002 with a
garbage HR, a null SpO2, one HR spike (135) and one normal reading, a non-JSON line and a
reading of an unknown patient P999.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import scoring  # noqa: E402
from streaming.enrichment import build_readings  # noqa: E402
from streaming.stream_job import batch_summaries  # noqa: E402
from streaming.validation import parse_kafka, reason_for, rejection_reason_col  # noqa: E402
from streaming.windows import windowed_vitals  # noqa: E402

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "vitals_fixture.jsonl"
LINES = FIXTURE.read_text(encoding="utf-8").splitlines()
IS_WINDOWS = os.name == "nt"  # Hadoop's local FS needs winutils.exe for checkpoints/writes

EXPECTED_REASONS = {
    0: None,
    1: None,
    2: None,
    3: None,
    4: "out_of_range:heart_rate",
    5: "missing_field:spo2",
    6: "malformed_record",
    7: "unknown_patient",
    8: None,
    9: None,
}


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("speed-layer-tests")
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # no Java on this machine
        pytest.skip(f"Spark unavailable: {exc}")
    yield session
    session.stop()


def kafka_like(spark):
    """The fixture as if read from Kafka (partition/offset/timestamp/value)."""
    rows = [(0, i, datetime(2026, 3, 1, 10, 2, 0), line) for i, line in enumerate(LINES)]
    return spark.createDataFrame(
        rows, "partition INT, offset LONG, timestamp TIMESTAMP, value STRING"
    )


def patients(spark):
    return spark.createDataFrame([("P001",), ("P002",)], "patient_id STRING")


def lab_risk(spark):
    return spark.createDataFrame(
        [("P002", 2, 1)], "patient_id STRING, lab_risk_points INT, lab_as_of_sim_day INT"
    )


# ------------------------------------------------------------------- scoring in Spark
def test_spark_scores_equal_python_scores(spark):
    values = sorted(
        {
            x
            for bands in scoring.BANDS.values()
            for u, _ in bands
            if u != scoring.INF
            for x in (u, u + 0.1, u + 1)
        }
    )
    df = spark.createDataFrame([(float(v),) for v in values], "x DOUBLE")
    for vital in scoring.SCORED_VITALS:
        got = df.select("x", scoring.vital_score_col(vital, F.col("x")).alias("s")).collect()
        for row in got:
            assert row["s"] == scoring.vital_score(vital, row["x"]), (vital, row["x"])
    nulls = spark.createDataFrame([(None,)], "x DOUBLE")
    assert nulls.select(scoring.vital_score_col("spo2", F.col("x"))).first()[0] == 0


def test_spark_tier_equals_python_tier(spark):
    pairs = [(t, m) for t in range(0, 13) for m in (0, 3)]
    df = spark.createDataFrame(pairs, "t INT, m INT")
    got = df.select("t", "m", scoring.risk_tier_col(F.col("t"), F.col("m")).alias("tier")).collect()
    for row in got:
        assert row["tier"] == scoring.risk_tier(row["t"], row["m"])


# ------------------------------------------------------------------------- validation
def test_validation_reasons_match_python_rules(spark):
    parsed = parse_kafka(kafka_like(spark))
    known = F.col("patient_id").isin("P001", "P002")
    rows = (
        parsed.withColumn("reason", rejection_reason_col(known)).orderBy("kafka_offset").collect()
    )
    for row in rows:
        assert row["reason"] == EXPECTED_REASONS[row["kafka_offset"]], row["kafka_offset"]
        if row["event_id"] is not None:
            record = row.asDict()
            assert reason_for(record, {"P001", "P002"}) == row["reason"]


def test_enrichment_joins_patients_and_lab_risk(spark):
    df = build_readings(kafka_like(spark), patients(spark), lab_risk(spark))
    rows = {r["kafka_offset"]: r for r in df.collect()}
    assert rows[7]["rejection_reason"] == "unknown_patient"
    p1 = rows[3]  # HR 90, SpO2 95, SBP 100, temp 37.5 -> 0 + 1 + 2 + 0
    assert (p1["news_score"], p1["lab_risk_points"], p1["total_score"], p1["risk_tier"]) == (
        3,
        0,
        3,
        "MEDIUM",
    )
    assert p1["map"] == pytest.approx(73.3) and p1["pulse_pressure"] == 40
    spike = rows[8]  # HR 135 scores 3; + 2 lab points -> total 5
    assert (spike["news_score"], spike["max_vital_score"], spike["total_score"]) == (3, 3, 5)
    assert spike["risk_tier"] == "HIGH" and spike["lab_as_of_sim_day"] == 1


def test_batch_summaries_pick_newest_reading_and_first_trigger(spark):
    df = build_readings(kafka_like(spark), patients(spark), lab_risk(spark))
    rows = {r["patient_id"]: r for r in batch_summaries(df).collect()}
    p2 = rows["P002"]
    assert p2["latest"]["heart_rate"] == 72  # 10:00:50 is newer than the spike at 10:00:40
    # the garbage HR 999 (DLQ) must not count as a critical vital - only the real spike 135
    assert p2["heart_rate_worst"] == 135
    assert p2["heart_rate_first3_at"] == datetime(2026, 3, 1, 10, 0, 40)
    assert p2["total_max"] == 5 and p2["total_first_high_at"] is not None
    assert p2["n_valid"] == 2
    assert sorted(r["rejection_reason"] for r in p2["rejected"]) == [
        "missing_field:spo2",
        "out_of_range:heart_rate",
    ]
    assert rows["P001"]["heart_rate_first3_at"] is None
    assert rows["P001"]["heart_rate_worst"] is None
    assert rows["P001"]["n_valid"] == 4  # the job dedupes before this step, the fixture does not
    assert rows["P999"]["n_valid"] == 0  # unknown patient: DLQ only, no status row
    assert rows[None]["rejected"][0]["rejection_reason"] == "malformed_record"


# ---------------------------------------------------------------------------- windows
def expected_p001_windows():
    """Hand-computed: 1-min windows sliding 30 s over e1 (10:00:05), e2 (10:00:35, sent twice),
    e3 (10:01:05). Values: (n, avg HR, min HR, max HR, window news)."""
    return {
        "09:59:30": (1, 70.0, 70, 70, 0),
        "10:00:00": (2, 75.0, 70, 80, 0),  # the duplicate of e2 is not counted
        "10:00:30": (2, 85.0, 80, 90, 1),  # SBP avg 105 -> 1
        "10:01:00": (1, 90.0, 90, 90, 3),  # SpO2 95 -> 1, SBP 100 -> 2
    }


def test_windowed_aggregation_matches_hand_computed_values(spark):
    valid = build_readings(kafka_like(spark), patients(spark), None).filter(
        F.col("rejection_reason").isNull()
    )
    rows = (
        windowed_vitals(valid, "1 minute", "30 seconds", "1 minute")
        .filter("patient_id = 'P001'")
        .collect()
    )
    got = {
        r["window_start"].strftime("%H:%M:%S"): (
            r["n_readings"],
            r["avg_heart_rate"],
            r["min_heart_rate"],
            r["max_heart_rate"],
            r["news_score"],
        )
        for r in rows
    }
    assert got == expected_p001_windows()


# -------------------------------------------------------- streaming integration (Linux)
@pytest.mark.skipif(IS_WINDOWS, reason="streaming checkpoints need winutils.exe on Windows")
def test_streaming_pipeline_end_to_end(spark, tmp_path):
    source = tmp_path / "in"
    source.mkdir()
    (source / "batch1.txt").write_text("\n".join(LINES) + "\n", encoding="utf-8")

    stream = (
        spark.readStream.format("text")
        .load(str(source))
        .select(
            F.lit(0).alias("partition"),
            F.lit(0).cast("long").alias("offset"),
            F.current_timestamp().alias("timestamp"),
            F.col("value"),
        )
    )
    readings_out: list = []
    windows_out: list = []

    readings = build_readings(stream, patients(spark), lab_risk(spark))
    q1 = (
        readings.writeStream.foreachBatch(lambda df, _id: readings_out.extend(df.collect()))
        .option("checkpointLocation", str(tmp_path / "cp1"))
        .start()
    )
    valid = build_readings(stream, patients(spark), None).filter(F.col("rejection_reason").isNull())
    q2 = (
        windowed_vitals(valid, "1 minute", "30 seconds", "1 minute")
        .writeStream.outputMode("update")
        .foreachBatch(lambda df, _id: windows_out.extend(df.collect()))
        .option("checkpointLocation", str(tmp_path / "cp2"))
        .start()
    )
    try:
        q1.processAllAvailable()
        q2.processAllAvailable()
    finally:
        q1.stop()
        q2.stop()

    reasons = sorted((r["rejection_reason"] or "valid") for r in readings_out)
    assert reasons.count("valid") == 6  # the duplicate is removed later (dedupe step in the job)
    assert "out_of_range:heart_rate" in reasons and "unknown_patient" in reasons

    p001 = {
        r["window_start"].strftime("%H:%M:%S"): (
            r["n_readings"],
            r["avg_heart_rate"],
            r["min_heart_rate"],
            r["max_heart_rate"],
            r["news_score"],
        )
        for r in windows_out
        if r["patient_id"] == "P001"
    }
    assert p001 == expected_p001_windows()


@pytest.mark.skipif(IS_WINDOWS, reason="Parquet writes need winutils.exe on Windows")
def test_lake_writer_is_idempotent_per_batch(spark, tmp_path):
    from streaming.sinks import write_lake

    valid = (
        build_readings(kafka_like(spark), patients(spark), None)
        .filter(F.col("rejection_reason").isNull())
        .select("event_id", "patient_id", "heart_rate", "sim_day")
    )
    lake, staging = tmp_path / "lake", tmp_path / "staging"
    assert write_lake(valid, str(lake), str(staging), batch_id=7, run_tag="aaaa1111") == 1
    assert write_lake(valid, str(lake), str(staging), batch_id=7, run_tag="aaaa1111") == 1  # replay
    files = sorted(f.name for f in (lake / "sim_day=1").glob("*.parquet"))
    assert files == ["part-aaaa1111-b0000000007-000.parquet"]
    assert spark.read.parquet(str(lake)).count() == valid.count()
    # a fresh checkpoint restarts batch ids at 0 - older files must survive
    write_lake(valid, str(lake), str(staging), batch_id=7, run_tag="bbbb2222")
    assert len(list((lake / "sim_day=1").glob("*.parquet"))) == 2
