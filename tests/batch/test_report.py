"""Risk merge and the daily report (task C8): before/after labs, ranking, sources, rendering."""

from __future__ import annotations

import csv
from datetime import UTC, datetime

from batch import report
from batch.report import LabLine, build_report

PATIENTS = [{"patient_id": f"P00{i}", "name": f"Patient {i}", "bed": f"B{i}"} for i in range(1, 6)]


def vitals(news: int, max_vital: int = 0, trend: str = "STABLE", **extra) -> dict:
    base = {
        "end_news_score": news,
        "end_max_vital_score": max_vital,
        "trend": trend,
        "avg_heart_rate": 80.0,
        "min_heart_rate": 70,
        "max_heart_rate": 95,
        "avg_spo2": 97.0,
        "min_spo2": 95,
        "max_spo2": 99,
        "avg_systolic_bp": 120.0,
        "min_systolic_bp": 110,
        "max_systolic_bp": 130,
        "avg_temperature": 36.8,
        "peak_news_score": news + 1,
        "hr_change": 0.0,
        "spo2_change": 0.0,
        "sbp_change": 0.0,
        "hr_series": [80.0, 81.0, None, 83.0],
        "spo2_series": [97.0, 96.0, 97.0, 96.0],
    }
    return base | extra


def build(**kw):
    args = {
        "day": 4,
        "patients": PATIENTS,
        "batch_vitals": {},
        "speed_status": {},
        "lab_before": {},
        "lab_after": {},
        "lab_lines": {},
    } | kw
    return {r.patient_id: r for r in build_report(**args)}, build_report(**args)


def test_labs_raise_an_occult_patient_from_low_to_high():
    """Normal vitals (NEWS 1) + new lab points 4 -> total 5 -> HIGH: the business question."""
    rows, _ = build(
        batch_vitals={"P001": vitals(1)},
        lab_after={"P001": (4, ["lactate:high", "wbc:high", "creatinine:high"])},
        lab_lines={"P001": [LabLine("lactate", 3.4, "HIGH"), LabLine("wbc", 14.0, "HIGH")]},
    )
    r = rows["P001"]
    assert (r.risk_before_labs, r.risk_after_labs, r.tier_change) == ("LOW", "HIGH", "UP")
    assert (r.total_before, r.total_after) == (1, 5)
    assert r.vitals_source == "batch" and r.vitals_sim_day == 3
    assert "lactate 3.4 ↑" in r.lab_summary and "wbc 14 ↑" in r.lab_summary


def test_normalised_labs_lower_the_tier():
    rows, _ = build(
        batch_vitals={"P002": vitals(2)},
        lab_before={"P002": 3},
        lab_after={"P002": (0, [])},
        lab_lines={"P002": [LabLine("lactate", 1.1, None)]},
    )
    r = rows["P002"]
    assert (r.risk_before_labs, r.risk_after_labs, r.tier_change) == ("HIGH", "LOW", "DOWN")
    assert r.lab_summary == "1 tests, all within range"


def test_patient_missing_from_the_file_keeps_previous_points():
    rows, _ = build(batch_vitals={"P003": vitals(2)}, lab_before={"P003": 2})
    r = rows["P003"]
    assert r.lab_points_before == r.lab_points_after == 2
    assert r.tier_change == "SAME" and r.lab_summary == "no labs in this file"


def test_single_vital_score_3_forces_medium_in_both_views():
    rows, _ = build(batch_vitals={"P004": vitals(3 - 1, max_vital=3)})
    assert rows["P004"].risk_before_labs == rows["P004"].risk_after_labs == "MEDIUM"


def test_vitals_source_falls_back_to_speed_layer_then_none():
    speed = {
        "P001": {
            "heart_rate": 88,
            "spo2": 95,
            "systolic_bp": 118,
            "temperature": 37.2,
            "news_score": 1,
            "max_vital_score": 1,
        }
    }
    rows, _ = build(speed_status=speed)
    assert rows["P001"].vitals_source == "speed" and rows["P001"].news_score == 1
    assert "speed layer" in rows["P001"].vitals_summary
    assert rows["P002"].vitals_source == "none" and rows["P002"].news_score == 0
    assert rows["P002"].hr_series == []


def test_ranking_by_tier_then_total_then_news_then_id():
    _, ranked = build(
        batch_vitals={
            "P001": vitals(1),  # LOW
            "P002": vitals(5),  # HIGH total 5
            "P003": vitals(4),  # MEDIUM -> HIGH with 1 lab point, total 5, news 4
            "P004": vitals(7),  # CRITICAL
            "P005": vitals(1),  # LOW (tie with P001 -> id order)
        },
        lab_after={"P003": (1, ["crp:high"])},
    )
    assert [r.patient_id for r in ranked] == ["P004", "P002", "P003", "P001", "P005"]
    assert [r.rank for r in ranked] == [1, 2, 3, 4, 5]


def test_worsening_trend_is_summarised():
    v = vitals(4, trend="WORSENING", hr_change=14.2, spo2_change=-3.5, sbp_change=0.2)
    rows, _ = build(batch_vitals={"P001": v})
    assert rows["P001"].trend == "WORSENING"
    assert rows["P001"].vitals_summary.endswith("WORSENING (HR +14, SpO2 -4)")


def test_alert_counts_are_attached():
    rows, _ = build(alerts_opened={"P002": 3})
    assert rows["P002"].alerts_opened == 3 and rows["P001"].alerts_opened == 0


def test_html_and_csv(tmp_path):
    _, ranked = build(
        batch_vitals={"P001": vitals(1)},
        lab_after={"P001": (4, ["lactate:high"])},
    )
    page = report.render_html(4, ranked, 0.0123, datetime(2026, 3, 1, tzinfo=UTC))
    assert "simulated day 4" in page and "labs_day_004.csv" in page
    assert "<svg" in page and "polyline" in page  # sparkline (gap splits it into two lines)
    assert "P001</b>: LOW → HIGH" in page
    assert "1.23% of readings" in page
    html_path, csv_path = report.report_paths(tmp_path, 4)
    report.write_html(html_path, page)
    report.write_csv(csv_path, ranked)
    assert html_path.read_text(encoding="utf-8") == page
    with csv_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == len(PATIENTS)
    assert rows[0]["patient_id"] == "P001" and rows[0]["abnormal_tests"] == "lactate:high"


def test_html_escapes_text():
    patients = [{"patient_id": "P001", "name": "<script>x</script>", "bed": "B1"}]
    ranked = build_report(4, patients, {}, {}, {}, {}, {})
    assert "<script>x" not in report.render_html(4, ranked, None, datetime.now(UTC))


def test_sparkline_edge_cases():
    assert "-" in report.sparkline([])
    assert "-" in report.sparkline([None, 5.0, None])
    flat = report.sparkline([5.0, 5.0, 5.0])
    assert "<polyline" in flat and "5-5" in flat
