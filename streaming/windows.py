"""Event-time windowed aggregation and trend analysis (task B5).

Spark part (``windowed_vitals``): sliding windows of ``STREAM_WINDOW_DURATION`` (2 min) every
``STREAM_WINDOW_SLIDE`` (30 s) per patient, over event time, with a ``STREAM_WATERMARK``
(1 min) lateness bound. Duplicates are removed by ``event_id`` within the watermark first,
so a re-sent reading is counted once. Output mode is *update*: each trigger emits the
windows whose aggregates changed, and the sink upserts them - dashboards see a window fill
up in near real time, and the final value is exact once the watermark passes the window.

Readings later than the watermark are dropped **here only**; they still reach the Parquet
lake, and the batch layer's full recompute measures what the speed layer missed
(``speed_batch_discrepancy_ratio``) - the Lambda trade-off made visible.

Python part (``analyse_patient_windows``): trend slopes (least squares over the last 5
windows), the worsening-trend flag and the sustained-abnormal counter. It runs in the
driver over at most ~8 rows per patient read back from ``vitals_window``, so it is exact,
cheap, and testable without Spark.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from common import scoring
from common.schemas import VITAL_FIELDS

INT_VITALS = ("heart_rate", "spo2", "systolic_bp", "diastolic_bp")


def windowed_vitals(valid_df: Any, window: str, slide: str, watermark: str) -> Any:
    """Per-patient sliding-window aggregates of valid readings (streaming or batch DataFrame)."""
    from pyspark.sql import functions as F

    watermarked = valid_df.withWatermark("event_time", watermark)
    # dropDuplicatesWithinWatermark exists for streams only; a batch (tests, backfill) dedupes fully
    if valid_df.isStreaming:
        deduped = watermarked.dropDuplicatesWithinWatermark(["event_id"])
    else:
        deduped = watermarked.dropDuplicates(["event_id"])
    aggs = [F.count(F.lit(1)).alias("n_readings"), F.min("sim_day").alias("sim_day")]
    for v in VITAL_FIELDS:
        aggs += [
            F.round(F.avg(v), 2).alias(f"avg_{v}"),
            F.min(v).alias(f"min_{v}"),
            F.max(v).alias(f"max_{v}"),
        ]
    out = deduped.groupBy("patient_id", F.window("event_time", window, slide)).agg(*aggs)
    out = out.select(
        "patient_id",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        *[c for c in out.columns if c not in ("patient_id", "window")],
    )
    avg_cols = {v: F.col(f"avg_{v}") for v in scoring.SCORED_VITALS}
    return out.withColumn("news_score", scoring.news_score_col(avg_cols))


# --------------------------------------------------------------------- trend analysis
@dataclass(frozen=True)
class WindowPoint:
    window_start: datetime
    window_end: datetime
    avg_heart_rate: float
    avg_spo2: float
    avg_systolic_bp: float
    news_score: int


@dataclass(frozen=True)
class TrendResult:
    slopes: dict[str, float | None]  # per minute
    flags: list[str]
    sustained_windows: int
    latest: WindowPoint

    @property
    def trend_flag(self) -> str:
        return ",".join(self.flags) if self.flags else "STABLE"


def analyse_patient_windows(points: list[WindowPoint]) -> TrendResult | None:
    """Trend over the most recent windows of one patient (``points`` in any order)."""
    if not points:
        return None
    pts = sorted(points, key=lambda p: p.window_start)
    recent = pts[-scoring.TREND_WINDOWS :]
    t0 = recent[0].window_start
    offsets = [(p.window_start - t0).total_seconds() for p in recent]
    slopes = {
        "heart_rate": scoring.slope_per_minute(
            zip(offsets, [p.avg_heart_rate for p in recent], strict=True)
        ),
        "spo2": scoring.slope_per_minute(zip(offsets, [p.avg_spo2 for p in recent], strict=True)),
        "systolic_bp": scoring.slope_per_minute(
            zip(offsets, [p.avg_systolic_bp for p in recent], strict=True)
        ),
    }
    if len(recent) < scoring.TREND_WINDOWS:
        # Too little history for a regression over 5 windows: report slopes, raise no flag.
        slopes_for_flags: dict[str, float | None] = {}
    else:
        slopes_for_flags = slopes
    news = [p.news_score for p in pts]
    flags = scoring.trend_flags(slopes_for_flags, news)
    return TrendResult(
        slopes=slopes,
        flags=flags,
        sustained_windows=scoring.consecutive_abnormal(news),
        latest=pts[-1],
    )
