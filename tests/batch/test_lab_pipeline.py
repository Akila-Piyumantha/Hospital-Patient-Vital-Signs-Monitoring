"""Lab file validation, abnormal flags and lab risk (task C8) - no database, no Airflow.

The files come from Member A's real lab generator, including its fault injection, so the tests
check the batch layer against exactly what it will receive.
"""

from __future__ import annotations

import random

import pytest

from batch import lab_pipeline as lp
from common.sim_clock import SimClock
from simulators.lab_generator import corrupt_rows, generate_lab_rows, write_lab_file
from simulators.patients import build_patients

PATIENTS = build_patients(20, 42, 300.0, 4, 2)
KNOWN = {p.patient_id for p in PATIENTS}
CLOCK = SimClock(epoch=1_767_225_600.0, day_seconds=300.0)
HEADER = "patient_id,test_type,result_value,reference_range,collected_at"


def row(**overrides):
    base = {
        "patient_id": "P001",
        "test_type": "lactate",
        "result_value": "3.1",
        "reference_range": "0.5-2.0",
        "collected_at": "2026-03-01T08:30:00Z",
    }
    return base | overrides


# ------------------------------------------------------------------------------ single rows
def test_contract_example_is_flagged_high():
    r = lp.validate_row(row(), KNOWN)
    assert isinstance(r, lp.LabResult)
    assert (r.ref_low, r.ref_high, r.abnormal_flag) == (0.5, 2.0, "HIGH")
    assert r.collected_at.isoformat() == "2026-03-01T08:30:00+00:00"


@pytest.mark.parametrize(
    "value, flag",
    [("0.5", None), ("2.0", None), ("2.01", "HIGH"), ("0.49", "LOW"), ("1.2", None)],
)
def test_abnormal_flag_boundaries_are_inclusive(value, flag):
    assert lp.validate_row(row(result_value=value), KNOWN).abnormal_flag == flag


@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"patient_id": "P999"}, "unknown_patient"),
        ({"patient_id": ""}, "unknown_patient"),
        ({"test_type": "troponin"}, "unknown_test_type"),
        ({"result_value": "N/A"}, "non_numeric_result"),
        ({"result_value": ""}, "non_numeric_result"),
        ({"result_value": "nan"}, "non_numeric_result"),
        ({"result_value": "-1"}, "negative_result"),
        ({"reference_range": "??"}, "bad_reference_range"),
        ({"reference_range": "5-1"}, "bad_reference_range"),
        ({"collected_at": ""}, "bad_collected_at"),
        ({"collected_at": "yesterday"}, "bad_collected_at"),
    ],
)
def test_row_rejections(overrides, reason):
    assert lp.validate_row(row(**overrides), KNOWN) == reason


def test_test_type_is_case_insensitive():
    assert lp.validate_row(row(test_type=" Lactate "), KNOWN).test_type == "lactate"


# ---------------------------------------------------------------------------- duplicates
def test_exact_duplicate_is_rejected_once():
    result = lp.validate_rows([row(), row()], KNOWN)
    assert len(result.rows) == 1
    assert result.reject_counts == {"duplicate_row": 1}


def test_conflicting_results_keep_the_latest_collection():
    older = row(result_value="3.1", collected_at="2026-03-01T08:30:00Z")
    newer = row(result_value="1.1", collected_at="2026-03-01T09:30:00Z")
    for order in ([older, newer], [newer, older]):
        result = lp.validate_rows(order, KNOWN)
        assert [r.result_value for r in result.rows] == [1.1]
        assert result.reject_counts == {"superseded_result": 1}


# ------------------------------------------------------------------------ whole files (A6)
def test_generated_file_is_fully_valid(tmp_path):
    rows = generate_lab_rows(3, PATIENTS, CLOCK, seed=42)
    path = write_lab_file(tmp_path, 3, rows)
    result = lp.validate_file(path, KNOWN)
    assert result.file_error is None
    assert result.rejected == []
    assert len(result.rows) == len(rows) == result.rows_in
    assert any(r.abnormal_flag for r in result.rows)


def test_corrupt_file_rejects_exactly_the_injected_defects(tmp_path):
    rows, defects = corrupt_rows(generate_lab_rows(5, PATIENTS, CLOCK, 42), random.Random(1))
    path = write_lab_file(tmp_path, 5, rows)
    result = lp.validate_file(path, KNOWN)
    assert result.file_error is None
    assert result.reject_counts == {
        "non_numeric_result": 1,
        "unknown_patient": 1,
        "bad_reference_range": 1,
        "bad_collected_at": 1,
        "duplicate_row": 1,
    }
    assert len(defects) == 5
    assert len(result.rows) == len(rows) - 5


def test_bad_schema_file_is_a_file_error(tmp_path):
    rows = generate_lab_rows(6, PATIENTS, CLOCK, 42)
    columns = ("patient_id", "result_value", "reference_range", "collected_at")
    path = write_lab_file(tmp_path, 6, rows, columns)
    result = lp.validate_file(path, KNOWN)
    assert result.file_error == "missing_columns:test_type"
    assert result.rows == []


@pytest.mark.parametrize(
    "content, error", [("", "empty_file"), (b"\xff\xfe\x00bad", "unreadable:UnicodeDecodeError")]
)
def test_empty_or_binary_file(tmp_path, content, error):
    path = tmp_path / "labs_day_007.csv"
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content)
    assert lp.validate_file(path, KNOWN).file_error == error


def test_header_with_spaces_and_extra_column_is_accepted():
    text = " patient_id , test_type,result_value,reference_range,collected_at,comment\n"
    text += "P001,crp,40,0-5,2026-03-01T08:30:00Z,urgent\n"
    result = lp.validate_text(text, KNOWN)
    assert result.file_error is None
    assert [(r.test_type, r.abnormal_flag) for r in result.rows] == [("crp", "HIGH")]


# ---------------------------------------------------------------------- files on disk / moves
def test_find_prefers_landing_then_processed_then_quarantine(tmp_path):
    name = lp.lab_filename(4)
    assert lp.find_lab_file(tmp_path, 4) is None
    for place in ("quarantine", "processed"):
        (tmp_path / place).mkdir()
        (tmp_path / place / name).write_text(HEADER)
        assert lp.find_lab_file(tmp_path, 4)[1] == place
    (tmp_path / name).write_text(HEADER)
    assert lp.find_lab_file(tmp_path, 4) == (tmp_path / name, "landing")


def test_quarantine_file_writes_reason(tmp_path):
    src = tmp_path / lp.lab_filename(6)
    src.write_text("x")
    target = lp.quarantine_file(src, tmp_path / "quarantine", "missing_columns:test_type")
    assert not src.exists() and target.exists()
    assert (target.parent / "labs_day_006.csv.reason.txt").read_text().strip() == (
        "missing_columns:test_type"
    )


def test_rejects_file_round_trip_and_cleanup(tmp_path):
    quarantine = tmp_path / "quarantine"
    path = lp.write_rejects(quarantine, 5, [(row(patient_id="P999"), "unknown_patient")])
    text = path.read_text(encoding="utf-8")
    assert text.splitlines()[0] == HEADER + ",reject_reason"
    assert "P999" in text and "unknown_patient" in text
    assert lp.write_rejects(quarantine, 5, []) is None  # replay of a clean file
    assert not path.exists()


# ------------------------------------------------------------------------------- lab risk
def _result(pid, test, flag):
    return lp.LabResult(pid, test, 1.0, 0.0, 2.0, flag, None)


def test_lab_risk_uses_shared_scoring_and_cap():
    rows = [
        _result("P001", "lactate", "HIGH"),  # +2
        _result("P001", "potassium", "LOW"),  # +2
        _result("P001", "wbc", "HIGH"),  # +1 -> 5, capped at 4
        _result("P002", "hemoglobin", "HIGH"),  # only LOW scores for hemoglobin
        _result("P002", "glucose", "LOW"),  # +1
        _result("P003", "crp", None),  # all normal -> row with 0 points
    ]
    risk = lp.lab_risk_by_patient(rows)
    assert risk["P001"] == (4, ["lactate:high", "potassium:low", "wbc:high"])
    assert risk["P002"] == (1, ["glucose:low", "hemoglobin:high"])
    assert risk["P003"] == (0, [])


def test_generated_occult_patients_get_lab_points():
    """Occult patients have normal vitals; only their labs reveal the risk (A's story)."""
    occult = [p for p in PATIENTS if p.occult_from_day is not None]
    assert occult
    day = max(p.occult_from_day for p in occult) + 2  # file day N holds labs of day N-1
    rows = generate_lab_rows(day, PATIENTS, CLOCK, 42)
    result = lp.validate_rows([{k: str(v) for k, v in r.items()} for r in rows], KNOWN)
    risk = lp.lab_risk_by_patient(result.rows)
    for p in occult:
        points, tests = risk[p.patient_id]
        assert points >= 2, (p.patient_id, tests)
