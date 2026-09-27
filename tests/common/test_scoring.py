"""Boundary tests for common/scoring.py (contract 4.5): every band edge from both sides."""

import pytest

from common import scoring as s

# (vital, value, expected sub-score) - each row is one side of a band edge in the plan's table.
BOUNDARIES = [
    ("heart_rate", 40, 3),
    ("heart_rate", 41, 1),
    ("heart_rate", 50, 1),
    ("heart_rate", 51, 0),
    ("heart_rate", 90, 0),
    ("heart_rate", 91, 1),
    ("heart_rate", 110, 1),
    ("heart_rate", 111, 2),
    ("heart_rate", 130, 2),
    ("heart_rate", 131, 3),
    ("spo2", 91, 3),
    ("spo2", 92, 2),
    ("spo2", 93, 2),
    ("spo2", 94, 1),
    ("spo2", 95, 1),
    ("spo2", 96, 0),
    ("spo2", 100, 0),
    ("systolic_bp", 90, 3),
    ("systolic_bp", 91, 2),
    ("systolic_bp", 100, 2),
    ("systolic_bp", 101, 1),
    ("systolic_bp", 110, 1),
    ("systolic_bp", 111, 0),
    ("systolic_bp", 219, 0),
    ("systolic_bp", 220, 3),
    ("temperature", 35.0, 3),
    ("temperature", 35.1, 1),
    ("temperature", 36.0, 1),
    ("temperature", 36.1, 0),
    ("temperature", 38.0, 0),
    ("temperature", 38.1, 1),
    ("temperature", 39.0, 1),
    ("temperature", 39.1, 2),
]


@pytest.mark.parametrize(("vital", "value", "expected"), BOUNDARIES)
def test_vital_score_band_edges(vital, value, expected):
    assert s.vital_score(vital, value) == expected


def test_missing_vital_scores_zero():
    assert s.vital_score("heart_rate", None) == 0


def test_window_averages_fall_into_the_next_band():
    assert s.vital_score("heart_rate", 90.4) == 1
    assert s.vital_score("spo2", 95.5) == 0
    assert s.vital_score("temperature", 38.05) == 1


def test_news_score_sums_sub_scores_and_ignores_diastolic():
    normal = {
        "heart_rate": 75,
        "spo2": 98,
        "systolic_bp": 120,
        "diastolic_bp": 20,
        "temperature": 36.8,
    }
    assert s.news_score(normal) == 0
    septic = {"heart_rate": 125, "spo2": 93, "systolic_bp": 88, "temperature": 39.4}
    assert s.vital_scores(septic) == {
        "heart_rate": 2,
        "spo2": 2,
        "systolic_bp": 3,
        "temperature": 2,
    }
    assert s.news_score(septic) == 9


@pytest.mark.parametrize(
    ("total", "max_vital", "tier"),
    [
        (0, 0, "LOW"),
        (2, 0, "LOW"),
        (3, 0, "MEDIUM"),
        (4, 0, "MEDIUM"),
        (5, 0, "HIGH"),
        (6, 0, "HIGH"),
        (7, 0, "CRITICAL"),
        (12, 3, "CRITICAL"),
        (3, 3, "MEDIUM"),  # single vital 3 alone -> at least MEDIUM
        (6, 3, "HIGH"),  # ... but never lowers a higher tier
    ],
)
def test_risk_tier(total, max_vital, tier):
    assert s.risk_tier(total, max_vital) == tier


def test_map():
    assert s.mean_arterial_pressure(120, 60) == pytest.approx(80.0)


# ------------------------------------------------------------------------------- labs
def test_parse_reference_range():
    assert s.parse_reference_range("0.5-2.0") == (0.5, 2.0)
    assert s.parse_reference_range(" 3.5-5.0 ") == (3.5, 5.0)
    for bad in ("", "abc", "5.0-3.5", "3.5"):
        with pytest.raises(ValueError):
            s.parse_reference_range(bad)


def test_abnormal_direction_is_inclusive_of_the_range():
    assert s.abnormal_direction(2.0, 0.5, 2.0) is None
    assert s.abnormal_direction(0.5, 0.5, 2.0) is None
    assert s.abnormal_direction(2.01, 0.5, 2.0) == "high"
    assert s.abnormal_direction(0.49, 0.5, 2.0) == "low"


def test_lab_points_per_contract():
    assert s.lab_risk_points([("lactate", "high")]) == 2
    assert s.lab_risk_points([("potassium", "low")]) == 2
    assert s.lab_risk_points([("potassium", "high")]) == 2
    assert s.lab_risk_points([("creatinine", "high"), ("wbc", "high")]) == 2
    assert s.lab_risk_points([("hemoglobin", "low"), ("glucose", "low")]) == 2
    # directions that do not score
    assert s.lab_risk_points([("lactate", "low"), ("hemoglobin", "high"), ("crp", "low")]) == 0
    assert s.lab_risk_points([("unknown_test", "high")]) == 0


def test_lab_points_cap_and_no_double_counting():
    everything = [("lactate", "high"), ("potassium", "high"), ("crp", "high"), ("wbc", "high")]
    assert s.lab_risk_points(everything) == s.LAB_POINTS_CAP == 4
    assert s.lab_risk_points([("crp", "high"), ("CRP", "high")]) == 1


# ----------------------------------------------------------------------------- trends
def test_slope_per_minute():
    # +1 unit every 30 s -> +2 per minute
    assert s.slope_per_minute([(0, 70), (30, 71), (60, 72), (90, 73)]) == pytest.approx(2.0)
    assert s.slope_per_minute([(0, 70)]) is None
    assert s.slope_per_minute([(0, 70), (0, 80)]) is None


def test_trend_flags():
    rising = {"heart_rate": 4.0, "spo2": -1.0, "systolic_bp": -5.0}
    assert s.trend_flags(rising, [0, 0, 0]) == ["HR_RISING", "SPO2_FALLING", "SBP_FALLING"]
    stable = {"heart_rate": 3.9, "spo2": -0.9, "systolic_bp": -4.9}
    assert s.trend_flags(stable, [1, 1, 1]) == []
    assert s.trend_flags({}, [1, 2, 3]) == ["NEWS_RISING"]
    assert s.trend_flags({}, [1, 2, 2]) == []
    assert s.trend_flags({}, [2, 3]) == []


def test_consecutive_abnormal():
    assert s.consecutive_abnormal([0, 3, 4, 5]) == 3
    assert s.consecutive_abnormal([3, 4, 2]) == 0
    assert s.consecutive_abnormal([]) == 0
