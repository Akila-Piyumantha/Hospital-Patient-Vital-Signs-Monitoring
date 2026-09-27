"""Trend analysis over stored windows, with hand-computed expectations."""

from datetime import UTC, datetime, timedelta

import pytest

from streaming.windows import WindowPoint, analyse_patient_windows

T0 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC)


def point(i, hr=75.0, spo2=97.0, sbp=120.0, news=0):
    start = T0 + timedelta(seconds=30 * i)
    return WindowPoint(start, start + timedelta(minutes=2), hr, spo2, sbp, news)


def test_stable_patient():
    res = analyse_patient_windows([point(i) for i in range(5)])
    assert res.trend_flag == "STABLE"
    assert res.slopes["heart_rate"] == pytest.approx(0.0)
    assert res.sustained_windows == 0


def test_deteriorating_patient_hand_computed():
    # per 30-s window: HR +3, SpO2 -1, SBP -3  ->  per minute: +6, -2, -6
    pts = [point(i, hr=80 + 3 * i, spo2=96 - i, sbp=110 - 3 * i, news=i) for i in range(5)]
    res = analyse_patient_windows(list(reversed(pts)))  # order of input must not matter
    assert res.slopes["heart_rate"] == pytest.approx(6.0)
    assert res.slopes["spo2"] == pytest.approx(-2.0)
    assert res.slopes["systolic_bp"] == pytest.approx(-6.0)
    assert res.flags == ["HR_RISING", "SPO2_FALLING", "SBP_FALLING", "NEWS_RISING"]
    assert res.sustained_windows == 2  # news 3, 4 at the end
    assert res.latest.window_start == pts[-1].window_start


def test_only_last_five_windows_count():
    old_spike = [point(0, hr=150)]
    flat = [point(i) for i in range(1, 6)]
    res = analyse_patient_windows(old_spike + flat)
    assert res.slopes["heart_rate"] == pytest.approx(0.0)


def test_short_history_reports_slopes_but_raises_no_slope_flag():
    res = analyse_patient_windows([point(0, hr=70), point(1, hr=90)])
    assert res.slopes["heart_rate"] == pytest.approx(40.0)
    assert res.flags == []


def test_sustained_abnormal_counter():
    res = analyse_patient_windows([point(i, news=n) for i, n in enumerate([1, 3, 3, 4])])
    assert res.sustained_windows == 3
    assert analyse_patient_windows([]) is None
