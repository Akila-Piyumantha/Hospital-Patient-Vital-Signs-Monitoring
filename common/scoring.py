"""Risk scoring shared by the speed layer and the batch layer (contract 4.5, owner: Member B).

A simplified NEWS2 adaptation that uses only the fields in the vitals feed (no respiration
rate or consciousness level) - for demonstration, **not clinically validated**.

Both layers must score a reading identically, otherwise the Lambda merge compares apples
with oranges. So the thresholds live here once, as data (``BANDS``), and are turned into

* plain Python functions (``vital_score``, ``news_score``, ``risk_tier`` ...) - unit tested,
  used by the alert engine and by Member C's lab/report code, and
* Spark column expressions (``vital_score_col``, ``news_score_col``) - used by the streaming
  job and by the batch Spark job. ``pyspark`` is imported lazily, so this module stays
  importable in services without Spark.

Band semantics: each band is ``(upper_inclusive, score)`` and the first band whose upper
bound is >= the value wins. That reads straight off the table in the plan for integer
vitals *and* behaves sensibly for window averages (e.g. HR 90.4 scores like 91-110).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Any

INF = math.inf

# vital -> ((upper_inclusive, score), ...). Diastolic BP is not scored (as in NEWS2).
BANDS: dict[str, tuple[tuple[float, int], ...]] = {
    # HR: <=40 -> 3 | 41-50 -> 1 | 51-90 -> 0 | 91-110 -> 1 | 111-130 -> 2 | >=131 -> 3
    "heart_rate": ((40, 3), (50, 1), (90, 0), (110, 1), (130, 2), (INF, 3)),
    # SpO2: <=91 -> 3 | 92-93 -> 2 | 94-95 -> 1 | >=96 -> 0
    "spo2": ((91, 3), (93, 2), (95, 1), (INF, 0)),
    # Systolic BP: <=90 -> 3 | 91-100 -> 2 | 101-110 -> 1 | 111-219 -> 0 | >=220 -> 3
    "systolic_bp": ((90, 3), (100, 2), (110, 1), (219, 0), (INF, 3)),
    # Temperature: <=35.0 -> 3 | 35.1-36.0 -> 1 | 36.1-38.0 -> 0 | 38.1-39.0 -> 1 | >=39.1 -> 2
    "temperature": ((35.0, 3), (36.0, 1), (38.0, 0), (39.0, 1), (INF, 2)),
}
SCORED_VITALS = tuple(BANDS)

# Risk tiers on total = news_score + lab_risk_points (lower bound inclusive, highest first).
TIERS: tuple[tuple[int, str], ...] = ((7, "CRITICAL"), (5, "HIGH"), (3, "MEDIUM"), (0, "LOW"))
TIER_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

# Lab points (contract 4.5). test_type -> {direction: points}; directions are "high" / "low".
LAB_POINTS: dict[str, dict[str, int]] = {
    "lactate": {"high": 2},
    "potassium": {"high": 2, "low": 2},
    "creatinine": {"high": 1},
    "wbc": {"high": 1},
    "crp": {"high": 1},
    "hemoglobin": {"low": 1},
    "glucose": {"high": 1, "low": 1},
}
LAB_POINTS_CAP = 4

# Alert thresholds (contract 4.5)
ALERT_TOTAL_SCORE = 5  # total >= 5 opens TOTAL_SCORE_HIGH
CRITICAL_VITAL_SCORE = 3  # any single vital scoring 3 opens <VITAL>_CRITICAL
SUSTAINED_NEWS_SCORE = 3  # window news >= 3 ...
SUSTAINED_WINDOWS = 3  # ... in >= 3 consecutive windows opens SUSTAINED_ABNORMAL
ALERT_COOLDOWN_SECONDS = 60

# Trend thresholds, per minute of window start time (independent of the slide interval).
# Chosen from the simulator: a sepsis ramp raises HR by ~11 bpm/min, a respiratory one drops
# SpO2 by ~2.7 %/min; baseline noise on 2-minute averages stays well below these values.
TREND_WINDOWS = 5
TREND_SLOPE_THRESHOLDS: dict[str, float] = {
    "heart_rate": 4.0,  # bpm/min rising
    "spo2": -1.0,  # %/min falling
    "systolic_bp": -5.0,  # mmHg/min falling
}
NEWS_RISING_WINDOWS = 3  # window news strictly increasing over the last 3 windows


# ----------------------------------------------------------------------- vitals (Python)
def vital_score(vital: str, value: float | None) -> int:
    """NEWS-style sub-score of one vital; ``None`` (missing) scores 0."""
    if value is None:
        return 0
    for upper, score in BANDS[vital]:
        if value <= upper:
            return score
    raise AssertionError("bands end with +inf")  # pragma: no cover


def vital_scores(reading: Mapping[str, Any]) -> dict[str, int]:
    """Sub-score per scored vital of a reading (``{"heart_rate": 1, ...}``)."""
    return {vital: vital_score(vital, reading.get(vital)) for vital in SCORED_VITALS}


def news_score(reading: Mapping[str, Any]) -> int:
    """Sum of the vital sub-scores (0..12)."""
    return sum(vital_scores(reading).values())


def mean_arterial_pressure(systolic: float, diastolic: float) -> float:
    """MAP = DBP + (SBP - DBP) / 3."""
    return diastolic + (systolic - diastolic) / 3.0


def risk_tier(total_score: int, max_vital_score: int = 0) -> str:
    """Tier of ``news_score + lab_risk_points``; a single vital scoring 3 forces >= MEDIUM."""
    tier = next(name for lower, name in TIERS if total_score >= lower)
    if max_vital_score >= CRITICAL_VITAL_SCORE and TIER_ORDER[tier] < TIER_ORDER["MEDIUM"]:
        tier = "MEDIUM"
    return tier


# --------------------------------------------------------------------------- labs (Python)
def parse_reference_range(text: str) -> tuple[float, float]:
    """``"0.5-2.0"`` -> ``(0.5, 2.0)``. Raises ``ValueError`` on anything else."""
    low, sep, high = text.strip().partition("-")
    if not sep:
        raise ValueError(f"reference range must look like 'low-high', got {text!r}")
    low_value, high_value = float(low), float(high)
    if low_value > high_value:
        raise ValueError(f"reference range low > high: {text!r}")
    return low_value, high_value


def abnormal_direction(value: float, low: float, high: float) -> str | None:
    """``"high"`` / ``"low"`` when outside ``[low, high]``, else ``None``."""
    if value > high:
        return "high"
    if value < low:
        return "low"
    return None


def lab_points_for(test_type: str, direction: str | None) -> int:
    """Points one abnormal result contributes (0 if normal or not a scored direction)."""
    if direction is None:
        return 0
    return LAB_POINTS.get(test_type.lower(), {}).get(direction, 0)


def lab_risk_points(abnormal: Iterable[tuple[str, str]]) -> int:
    """Total lab points for ``(test_type, direction)`` pairs, each test counted once, capped.

    Counting each test once means a re-sent or duplicated result cannot inflate the score.
    """
    best: dict[str, int] = {}
    for test_type, direction in abnormal:
        key = test_type.lower()
        best[key] = max(best.get(key, 0), lab_points_for(key, direction))
    return min(LAB_POINTS_CAP, sum(best.values()))


# ------------------------------------------------------------------------ trends (Python)
def slope_per_minute(points: Iterable[tuple[float, float]]) -> float | None:
    """Least-squares slope of ``(t_seconds, value)`` points, in value units per minute.

    ``None`` with fewer than two points or when all points share one timestamp.
    """
    pts = [(t, v) for t, v in points if v is not None]
    n = len(pts)
    if n < 2:
        return None
    mean_t = sum(t for t, _ in pts) / n
    mean_v = sum(v for _, v in pts) / n
    sxx = sum((t - mean_t) ** 2 for t, _ in pts)
    if sxx == 0:
        return None
    sxy = sum((t - mean_t) * (v - mean_v) for t, v in pts)
    return sxy / sxx * 60.0


def trend_flags(slopes: Mapping[str, float | None], window_news: list[int]) -> list[str]:
    """Worsening-trend flags from per-minute slopes and the chronological window news scores.

    Returns e.g. ``["HR_RISING", "SPO2_FALLING"]``; an empty list means stable.
    """
    flags: list[str] = []
    hr, spo2, sbp = slopes.get("heart_rate"), slopes.get("spo2"), slopes.get("systolic_bp")
    if hr is not None and hr >= TREND_SLOPE_THRESHOLDS["heart_rate"]:
        flags.append("HR_RISING")
    if spo2 is not None and spo2 <= TREND_SLOPE_THRESHOLDS["spo2"]:
        flags.append("SPO2_FALLING")
    if sbp is not None and sbp <= TREND_SLOPE_THRESHOLDS["systolic_bp"]:
        flags.append("SBP_FALLING")
    recent = window_news[-NEWS_RISING_WINDOWS:]
    rising = all(a < b for a, b in zip(recent, recent[1:], strict=False))
    if len(recent) == NEWS_RISING_WINDOWS and rising:
        flags.append("NEWS_RISING")
    return flags


def consecutive_abnormal(window_news: list[int], threshold: int = SUSTAINED_NEWS_SCORE) -> int:
    """How many of the most recent windows in a row have news >= ``threshold``."""
    count = 0
    for score in reversed(window_news):
        if score < threshold:
            break
        count += 1
    return count


# ------------------------------------------------------------------ Spark column builders
def vital_score_col(vital: str, column: Any = None) -> Any:
    """Spark ``Column`` equal to ``vital_score(vital, value)``; nulls score 0."""
    from pyspark.sql import functions as F

    col = F.col(vital) if column is None else column
    expr = None
    for upper, score in BANDS[vital]:
        cond = col <= F.lit(upper) if upper != INF else F.lit(True)
        expr = F.when(cond, F.lit(score)) if expr is None else expr.when(cond, F.lit(score))
    return F.when(col.isNull(), F.lit(0)).otherwise(expr)


def news_score_col(columns: Mapping[str, Any] | None = None) -> Any:
    """Spark ``Column`` equal to ``news_score``; ``columns``: vital -> Column (e.g. averages)."""
    from functools import reduce

    columns = columns or {}
    parts = [vital_score_col(v, columns.get(v)) for v in SCORED_VITALS]
    return reduce(lambda a, b: a + b, parts)


def risk_tier_col(total: Any, max_vital: Any) -> Any:
    """Spark ``Column`` equal to ``risk_tier(total, max_vital)``."""
    from pyspark.sql import functions as F

    expr = None
    for lower, name in TIERS:
        cond = total >= F.lit(lower)
        expr = F.when(cond, F.lit(name)) if expr is None else expr.when(cond, F.lit(name))
    expr = expr.otherwise(F.lit("LOW"))
    return F.when((max_vital >= CRITICAL_VITAL_SCORE) & (expr == "LOW"), F.lit("MEDIUM")).otherwise(
        expr
    )
