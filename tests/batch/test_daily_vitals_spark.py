"""Local-mode Spark test of the batch job (task C4) on an in-memory day of readings.

Skipped without pyspark/Java (CI job ``batch-serving-tests`` runs it). No Parquet I/O, so it also
runs on Windows without winutils.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from batch.daily_vitals_job import (  # noqa: E402
    batch_windows,
    complete_windows,
    daily_rows,
    reconcile_windows,
    score_readings,
)

DAY_START = datetime(2026, 3, 1, 10, 0, tzinfo=UTC)
SCHEMA = (
    "event_id STRING, patient_id STRING, heart_rate INT, spo2 INT, systolic_bp INT, "
    "diastolic_bp INT, temperature DOUBLE, event_time TIMESTAMP, sim_day INT"
)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("batch-layer-tests")
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # no Java on this machine
        pytest.skip(f"Spark unavailable: {exc}")
    yield session
    session.stop()


def readings():
    """300 s day, one reading per patient every 2 s. P001 deteriorates, P002 is stable."""
    rows = []
    for k in range(150):
        t = DAY_START + timedelta(seconds=2 * k)
        frac = k / 149
        rows.append(
            (
                str(uuid.uuid4()),
                "P001",
                int(80 + 40 * frac),
                int(97 - 6 * frac),
                int(125 - 30 * frac),
                80,
                36.8 + 2 * frac,
                t,
                3,
            )
        )
        rows.append((str(uuid.uuid4()), "P002", 75, 98, 120, 80, 36.7, t, 3))
    rows.append(rows[10])  # cross-batch duplicate in the lake
    return rows


@pytest.fixture(scope="module")
def scored(spark):
    return score_readings(spark.createDataFrame(readings(), SCHEMA)).cache()


def test_dedupe_and_daily_rows(scored):
    assert scored.count() == 300
    rows = {r["patient_id"]: r for r in daily_rows(scored, 0.25, 24)}
    p1, p2 = rows["P001"], rows["P002"]
    assert p1["n_readings"] == p2["n_readings"] == 150
    assert (p1["min_heart_rate"], p1["max_heart_rate"]) == (80, 120)
    assert p1["trend"] == "WORSENING" and p1["hr_change"] > 20 and p1["spo2_change"] < -3
    assert p1["end_news_score"] >= 5 and p1["peak_news_score"] >= p1["end_news_score"]
    assert p2["trend"] == "STABLE" and p2["end_news_score"] == 0 and p2["pct_news_ge3"] == 0
    assert p2["pct_abnormal_hr"] == 0 and p1["pct_abnormal_hr"] > 0.3
    assert len(p1["hr_series"]) == 24 and p1["hr_series"][0] < p1["hr_series"][-1]


def test_windows_match_speed_layer_alignment(scored):
    windows = batch_windows(scored, "2 minutes", "30 seconds")
    first = DAY_START
    last = DAY_START + timedelta(seconds=298)
    full = complete_windows(windows, first, last, 120)
    assert {start for _, start in full} == {DAY_START + timedelta(seconds=30 * k) for k in range(6)}
    assert all(n == 60 for n, _ in full.values())  # 120 s / 2 s
    same = reconcile_windows(full, dict(full))
    assert same.discrepancy_ratio == 0
    late = {k: (n - 1, hr) for k, (n, hr) in full.items()}  # one late reading per window
    assert reconcile_windows(full, late).discrepancy_ratio == pytest.approx(1 / 60)
