"""Pure-Python parts of the batch Spark job (task C4): trend label, scoring, reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from batch.daily_vitals_job import (
    complete_windows,
    day_trend,
    end_of_day_score,
    parse_duration,
    reconcile_windows,
)

T0 = datetime(2026, 3, 1, 10, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "hr, spo2, sbp, expected",
    [
        (0, 0, 0, "STABLE"),
        (9.9, -2.9, -14.9, "STABLE"),
        (10, 0, 0, "WORSENING"),  # HR up 10
        (0, -3, 0, "WORSENING"),  # SpO2 down 3
        (0, 0, -15, "WORSENING"),  # SBP down 15
        (-10, 0, 0, "IMPROVING"),
        (0, 3, 15, "IMPROVING"),
        (-12, -4, 0, "WORSENING"),  # any worsening wins
        (None, None, None, "STABLE"),
    ],
)
def test_day_trend(hr, spo2, sbp, expected):
    assert day_trend(hr, spo2, sbp) == expected


def test_end_of_day_score_uses_shared_bands():
    # HR 112 -> 2, SpO2 93 -> 2, SBP 105 -> 1, temp 38.5 -> 1  => NEWS 6, max 2
    assert end_of_day_score(
        {"heart_rate": 112, "spo2": 93, "systolic_bp": 105, "temperature": 38.5}
    ) == (6, 2)
    assert end_of_day_score({"heart_rate": None, "spo2": 90}) == (3, 3)


def test_parse_duration():
    assert parse_duration("2 minutes") == 120
    assert parse_duration("30 seconds") == 30
    assert parse_duration("1 hour") == 3600


def w(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


def test_reconcile_identical_layers():
    batch = {("P001", w(0)): (60, 80.0), ("P001", w(0.5)): (60, 81.0)}
    r = reconcile_windows(batch, dict(batch))
    assert r.discrepancy_ratio == 0.0 and r.count_abs_diff == 0 and r.windows_missing == 0
    assert r.mean_abs_diff_hr == 0.0


def test_reconcile_counts_late_readings_and_missing_windows():
    batch = {
        ("P001", w(0)): (60, 80.0),
        ("P001", w(0.5)): (60, 82.0),
        ("P002", w(0)): (60, 70.0),
    }
    speed = {
        ("P001", w(0)): (57, 80.5),  # 3 late readings dropped by the watermark
        ("P001", w(0.5)): (60, 81.0),
        # P002 window never written (speed layer down)
        ("P009", w(0)): (10, 70.0),  # not in the batch set: ignored
    }
    r = reconcile_windows(batch, speed)
    assert (r.windows_compared, r.windows_missing) == (3, 1)
    assert (r.batch_readings, r.speed_readings, r.count_abs_diff) == (180, 117, 63)
    assert r.discrepancy_ratio == pytest.approx(63 / 180)
    assert r.mean_abs_diff_hr == pytest.approx(0.75)


def test_reconcile_empty():
    r = reconcile_windows({}, {})
    assert r.discrepancy_ratio == 0.0 and r.mean_abs_diff_hr is None


def test_complete_windows_drop_the_day_edges():
    # data from 10:00:10 to 10:04:50; 2-min windows every 30 s
    first, last = T0 + timedelta(seconds=10), T0 + timedelta(minutes=4, seconds=50)
    windows = {("P001", w(m)): m for m in (-1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5)}
    kept = complete_windows(windows, first, last, 120)
    assert sorted(v for v in kept.values()) == [0.5, 1, 1.5, 2, 2.5]
