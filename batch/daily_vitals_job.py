"""Batch Spark job (task C4): recompute one simulated day of vitals from the Parquet lake.

    python -m batch.daily_vitals_job --day 3          # what the DAG task batch_vitals_job runs
    python -m batch.daily_vitals_job --day 3 --dry-run

The lake (``data/lake/vitals/sim_day=N``, written by the speed layer) is the immutable master
dataset. The job

1. reads the day, removes cross-batch duplicates (``dropDuplicates(event_id)``) and re-scores every
   reading with ``common.scoring`` - the same rules as the speed layer;
2. writes one row per patient to ``batch_vitals_daily``: min/max/avg per vital, share of time in an
   abnormal band, peak NEWS, the NEWS of the end-of-day averages, first-vs-last part change and a
   trend label, plus bucketed series for the report's sparklines;
3. **reconciles** the day against the speed layer: it rebuilds the same 2-min/30-s event-time
   windows the streaming job builds and compares their reading counts (and average HR) with
   ``vitals_window``. ``discrepancy_ratio = sum |n_batch - n_speed| / sum n_batch`` over the
   windows that lie completely inside the day. It is written to ``speed_batch_reconciliation`` and
   pushed as ``speed_batch_discrepancy_ratio``. It is > 0 when the speed layer dropped late
   readings (watermark), missed data while it was down, or drifted in logic - the reason the batch
   layer exists in a Lambda architecture.

Everything that is not Spark (trend label, end-of-day score, reconciliation arithmetic) is plain
Python over at most a few hundred rows and is unit-tested without Spark.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common import scoring
from common.config import env_str

VITALS = ("heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature")
SHORT = {"heart_rate": "hr", "spo2": "spo2", "systolic_bp": "sbp", "temperature": "temp"}

# Day-level trend label: change of the end-of-day average against the start-of-day average.
# Report-level thresholds (not part of the scoring contract): a clear clinical move within a day.
TREND_THRESHOLDS = {"heart_rate": 10.0, "spo2": -3.0, "systolic_bp": -15.0}


# ------------------------------------------------------------------------ pure Python part
def day_trend(hr_change: float | None, spo2_change: float | None, sbp_change: float | None) -> str:
    """``WORSENING`` if any vital moved past its threshold the wrong way, ``IMPROVING`` if any
    moved that far the right way (and none the wrong way), else ``STABLE``."""
    changes = {"heart_rate": hr_change, "spo2": spo2_change, "systolic_bp": sbp_change}
    worse = better = False
    for vital, threshold in TREND_THRESHOLDS.items():
        delta = changes[vital]
        if delta is None:
            continue
        signed = delta / threshold  # > 1: moved past the threshold in the "worse" direction
        worse |= signed >= 1
        better |= signed <= -1
    if worse:
        return "WORSENING"
    return "IMPROVING" if better else "STABLE"


def end_of_day_score(averages: dict[str, float | None]) -> tuple[int, int]:
    """(NEWS, highest single-vital score) of the end-of-day averages."""
    scores = [scoring.vital_score(v, averages.get(v)) for v in scoring.SCORED_VITALS]
    return sum(scores), max(scores)


@dataclass(frozen=True)
class Reconciliation:
    windows_compared: int
    windows_missing: int
    batch_readings: int
    speed_readings: int
    count_abs_diff: int
    mean_abs_diff_hr: float | None
    discrepancy_ratio: float


def reconcile_windows(
    batch: dict[tuple[str, datetime], tuple[int, float | None]],
    speed: dict[tuple[str, datetime], tuple[int, float | None]],
) -> Reconciliation:
    """Compare per-(patient, window_start) ``(n_readings, avg_heart_rate)`` of both layers.

    ``batch`` holds the windows to compare (already restricted to complete windows); a window
    the speed layer never wrote counts with ``n = 0``.
    """
    diff = missing = n_batch = n_speed = 0
    hr_diffs: list[float] = []
    for key, (bn, bhr) in batch.items():
        sn, shr = speed.get(key, (0, None))
        if key not in speed:
            missing += 1
        n_batch += bn
        n_speed += sn
        diff += abs(bn - sn)
        if bhr is not None and shr is not None:
            hr_diffs.append(abs(bhr - shr))
    return Reconciliation(
        windows_compared=len(batch),
        windows_missing=missing,
        batch_readings=n_batch,
        speed_readings=n_speed,
        count_abs_diff=diff,
        mean_abs_diff_hr=round(sum(hr_diffs) / len(hr_diffs), 3) if hr_diffs else None,
        discrepancy_ratio=round(diff / n_batch, 6) if n_batch else 0.0,
    )


def complete_windows(
    windows: dict[tuple[str, datetime], Any],
    day_first: datetime,
    day_last: datetime,
    window_seconds: float,
) -> dict[tuple[str, datetime], Any]:
    """Windows lying completely inside the day's data (edge windows also hold other days)."""
    return {
        key: value
        for key, value in windows.items()
        if key[1] >= day_first and key[1].timestamp() + window_seconds <= day_last.timestamp() + 1
    }


def parse_duration(text: str) -> float:
    """``"2 minutes"`` / ``"30 seconds"`` (Spark interval strings) -> seconds."""
    number, unit = text.split()
    factor = {"second": 1, "minute": 60, "hour": 3600}[unit.rstrip("s")]
    return float(number) * factor


# ------------------------------------------------------------------------------ Spark part
def build_spark(master: str, driver_memory: str) -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master(master)
        .appName("vitals-batch-layer")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", driver_memory)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def lake_partition(lake_dir: Path, day: int) -> Path | None:
    """The lake folder of ``day``, or ``None`` when the speed layer wrote nothing that day."""
    part = lake_dir / f"sim_day={day}"
    if not part.is_dir() or not any(part.glob("*.parquet")):
        return None
    return part


def read_day(spark: Any, lake_dir: Path, part: Path) -> Any:
    return spark.read.option("basePath", str(lake_dir)).parquet(str(part))


def score_readings(df: Any) -> Any:
    """Deduplicated readings with the NEWS sub-scores recomputed from the raw vitals."""
    from pyspark.sql import functions as F

    df = df.dropDuplicates(["event_id"])
    for v in scoring.SCORED_VITALS:
        df = df.withColumn(f"{v}_score", scoring.vital_score_col(v))
    return df.withColumn(
        "batch_news_score", sum((F.col(f"{v}_score") for v in scoring.SCORED_VITALS), F.lit(0))
    )


def daily_rows(scored: Any, end_fraction: float, buckets: int) -> list[dict]:
    """One dict per patient for ``batch_vitals_daily`` (Spark aggregates + Python scoring)."""
    from pyspark.sql import functions as F

    ts = F.col("event_time").cast("double")
    bounds = scored.agg(F.min(ts).alias("t0"), F.max(ts).alias("t1")).first()
    t0, t1 = bounds["t0"], bounds["t1"]
    span = max(t1 - t0, 1.0)
    frac = (ts - F.lit(t0)) / F.lit(span)
    df = scored.withColumn("_frac", frac).withColumn(
        "_bucket", F.least(F.floor(F.col("_frac") * buckets), F.lit(buckets - 1)).cast("int")
    )
    start_part = F.col("_frac") < end_fraction
    end_part = F.col("_frac") >= 1 - end_fraction

    aggs = [
        F.count(F.lit(1)).alias("n_readings"),
        F.min("event_time").alias("first_reading_at"),
        F.max("event_time").alias("last_reading_at"),
        F.max("batch_news_score").alias("peak_news_score"),
        F.avg((F.col("batch_news_score") >= scoring.SUSTAINED_NEWS_SCORE).cast("int")).alias(
            "pct_news_ge3"
        ),
    ]
    for v in VITALS:
        aggs += [
            F.avg(v).alias(f"avg_{v}"),
            F.min(v).alias(f"min_{v}"),
            F.max(v).alias(f"max_{v}"),
            F.avg(F.when(end_part, F.col(v))).alias(f"end_{v}"),
            F.avg(F.when(start_part, F.col(v))).alias(f"start_{v}"),
        ]
    for v in scoring.SCORED_VITALS:
        aggs.append(F.avg((F.col(f"{v}_score") > 0).cast("int")).alias(f"pct_abnormal_{SHORT[v]}"))
    per_patient = {
        r["patient_id"]: r.asDict() for r in df.groupBy("patient_id").agg(*aggs).collect()
    }

    series_rows = (
        df.groupBy("patient_id", "_bucket")
        .agg(*[F.avg(v).alias(v) for v in ("heart_rate", "spo2", "systolic_bp")])
        .collect()
    )
    series: dict[str, dict[str, list[float | None]]] = {}
    for r in series_rows:
        per = series.setdefault(
            r["patient_id"], {v: [None] * buckets for v in ("heart_rate", "spo2", "systolic_bp")}
        )
        for v in per:
            per[v][r["_bucket"]] = None if r[v] is None else round(r[v], 1)

    out = []
    for pid, r in sorted(per_patient.items()):
        end_avgs = {v: r[f"end_{v}"] for v in VITALS}
        news, max_vital = end_of_day_score(end_avgs)
        changes = {
            v: None
            if r[f"end_{v}"] is None or r[f"start_{v}"] is None
            else round(r[f"end_{v}"] - r[f"start_{v}"], 2)
            for v in ("heart_rate", "spo2", "systolic_bp")
        }
        row = {
            "patient_id": pid,
            "n_readings": r["n_readings"],
            "first_reading_at": r["first_reading_at"],
            "last_reading_at": r["last_reading_at"],
            "peak_news_score": r["peak_news_score"],
            "pct_news_ge3": round(r["pct_news_ge3"], 4),
            "end_news_score": news,
            "end_max_vital_score": max_vital,
            "hr_change": changes["heart_rate"],
            "spo2_change": changes["spo2"],
            "sbp_change": changes["systolic_bp"],
            "trend": day_trend(changes["heart_rate"], changes["spo2"], changes["systolic_bp"]),
            "hr_series": series.get(pid, {}).get("heart_rate", []),
            "spo2_series": series.get(pid, {}).get("spo2", []),
            "sbp_series": series.get(pid, {}).get("systolic_bp", []),
        }
        for v in VITALS:
            avg = r[f"avg_{v}"]
            row[f"avg_{v}"] = None if avg is None else round(avg, 2)
            row[f"min_{v}"] = r[f"min_{v}"]
            row[f"max_{v}"] = r[f"max_{v}"]
        for v in scoring.SCORED_VITALS:
            row[f"pct_abnormal_{SHORT[v]}"] = round(r[f"pct_abnormal_{SHORT[v]}"], 4)
        out.append(row)
    return out


def batch_windows(scored: Any, window: str, slide: str) -> dict[tuple[str, datetime], tuple]:
    """The speed layer's windows, rebuilt from the lake: (patient, start) -> (n, avg HR)."""
    from pyspark.sql import functions as F

    rows = (
        scored.groupBy("patient_id", F.window("event_time", window, slide).alias("w"))
        .agg(F.count(F.lit(1)).alias("n"), F.avg("heart_rate").alias("avg_hr"))
        .select("patient_id", F.col("w.start").alias("start"), "n", "avg_hr")
        .collect()
    )
    return {(r["patient_id"], _utc(r["start"])): (r["n"], r["avg_hr"]) for r in rows}


def _utc(value: datetime) -> datetime:
    """Spark hands timestamps to Python as naive *local* datetimes; make them aware UTC."""
    if value.tzinfo is None:
        return datetime.fromtimestamp(value.timestamp(), tz=UTC)
    return value.astimezone(UTC)


# ---------------------------------------------------------------------------- persistence
DAILY_COLUMNS = (
    "sim_day",
    "patient_id",
    "n_readings",
    "first_reading_at",
    "last_reading_at",
    *[f"{a}_{v}" for v in VITALS for a in ("avg", "min", "max")],
    "pct_abnormal_hr",
    "pct_abnormal_spo2",
    "pct_abnormal_sbp",
    "pct_abnormal_temp",
    "pct_news_ge3",
    "peak_news_score",
    "end_news_score",
    "end_max_vital_score",
    "hr_change",
    "spo2_change",
    "sbp_change",
    "trend",
    "hr_series",
    "spo2_series",
    "sbp_series",
)
_ARRAY_COLUMNS = {"hr_series", "spo2_series", "sbp_series"}


def write_daily(cur: Any, day: int, rows: list[dict]) -> int:
    from psycopg2.extras import execute_values

    cur.execute("DELETE FROM batch_vitals_daily WHERE sim_day = %s", (day,))
    if not rows:
        return 0
    template = (
        "("
        + ", ".join(
            "%s::double precision[]" if c in _ARRAY_COLUMNS else "%s" for c in DAILY_COLUMNS
        )
        + ")"
    )
    values = [
        tuple(
            day if c == "sim_day" else (_utc(r[c]) if isinstance(r[c], datetime) else r[c])
            for c in DAILY_COLUMNS
        )
        for r in rows
    ]
    execute_values(
        cur,
        f"INSERT INTO batch_vitals_daily ({', '.join(DAILY_COLUMNS)}) VALUES %s",
        values,
        template=template,
    )
    return len(values)


def fetch_speed_windows(
    cur: Any, start: datetime, end: datetime
) -> dict[tuple[str, datetime], tuple[int, float | None]]:
    cur.execute(
        "SELECT patient_id, window_start, n_readings, avg_heart_rate FROM vitals_window "
        "WHERE window_start >= %s AND window_start < %s",
        (start, end),
    )
    return {(r[0], _utc(r[1])): (r[2], r[3]) for r in cur.fetchall()}


def write_reconciliation(
    cur: Any, day: int, recon: Reconciliation, lake_readings: int, lake_duplicates: int
) -> None:
    fields = asdict(recon)
    cols = ["sim_day", *fields, "lake_readings", "lake_duplicates"]
    cur.execute(
        f"INSERT INTO speed_batch_reconciliation ({', '.join(cols)}) "
        f"VALUES ({', '.join(['%s'] * len(cols))}) ON CONFLICT (sim_day) DO UPDATE SET "
        + ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[1:])
        + ", computed_at = now()",
        (day, *fields.values(), lake_readings, lake_duplicates),
    )


# ------------------------------------------------------------------------------------ run
@dataclass
class JobResult:
    sim_day: int
    status: str  # "ok" | "no_data"
    lake_rows: int = 0
    readings: int = 0
    patients: int = 0
    reconciliation: dict | None = None
    duration_s: float = 0.0


def run(settings: Any, day: int, dry_run: bool = False, spark: Any = None) -> JobResult:
    """Recompute ``day``; ``settings`` is a ``batch.settings.BatchSettings``."""
    from batch import db

    started = time.time()
    part = lake_partition(settings.lake_dir, day)
    if part is None:  # no Spark start-up for an empty day
        if not dry_run:
            with db.connect(settings.pg) as conn, conn.cursor() as cur:
                write_daily(cur, day, [])
        return JobResult(day, "no_data", duration_s=round(time.time() - started, 1))

    own_spark = spark is None
    spark = spark or build_spark(settings.spark_master, settings.spark_driver_memory)
    try:
        raw = read_day(spark, settings.lake_dir, part).cache()
        lake_rows = raw.count()
        scored = score_readings(raw).cache()
        readings = scored.count()
        rows = daily_rows(scored, settings.end_of_day_fraction, settings.sparkline_buckets)

        window = env_str("STREAM_WINDOW_DURATION", "2 minutes")
        slide = env_str("STREAM_WINDOW_SLIDE", "30 seconds")
        first = min(_utc(r["first_reading_at"]) for r in rows)
        last = max(_utc(r["last_reading_at"]) for r in rows)
        batch_w = complete_windows(
            batch_windows(scored, window, slide), first, last, parse_duration(window)
        )
        with db.connect(settings.pg) as conn, conn.cursor() as cur:
            speed_w = fetch_speed_windows(cur, first, last)
            recon = reconcile_windows(batch_w, speed_w)
            if not dry_run:
                write_daily(cur, day, rows)
                write_reconciliation(cur, day, recon, readings, lake_rows - readings)
            else:
                conn.rollback()
        scored.unpersist()
        raw.unpersist()
        return JobResult(
            day,
            "ok",
            lake_rows=lake_rows,
            readings=readings,
            patients=len(rows),
            reconciliation=asdict(recon),
            duration_s=round(time.time() - started, 1),
        )
    finally:
        if own_spark:
            spark.stop()


def main(argv: list[str] | None = None) -> int:
    from batch.settings import BatchSettings
    from common.logging_setup import configure_logging, get_logger

    parser = argparse.ArgumentParser(description="Recompute one simulated day from the lake")
    parser.add_argument("--day", type=int, required=True, help="vitals sim day (lake partition)")
    parser.add_argument("--dry-run", action="store_true", help="compute, write nothing")
    args = parser.parse_args(argv)
    settings = BatchSettings.from_env()
    configure_logging("batch-vitals-job", settings.base.log_level, default_stage="processing")
    result = run(settings, args.day, args.dry_run)
    get_logger("batch.daily_vitals_job", "processing").info(
        "batch_day_recomputed", **asdict(result)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
