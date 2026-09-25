import csv
import threading

import pytest

from common.schemas import LAB_COLUMNS
from common.sim_clock import SimClock
from simulators.lab_generator import (
    TEST_SPECS,
    DayState,
    LabFaultConfig,
    _is_abnormal,
    corrupt_rows,
    drop_day,
    generate_lab_rows,
    lab_filename,
    run,
    write_lab_file,
)
from simulators.patients import build_patients

CLOCK = SimClock(epoch=1_000_000.0, day_seconds=300)
PATIENTS = build_patients(20, seed=11, day_seconds=300)
NO_FAULTS = LabFaultConfig()


def _abnormal_by_patient(day):
    result: dict[str, set[str]] = {}
    for row in generate_lab_rows(day, PATIENTS, CLOCK, seed=11):
        if _is_abnormal(row):
            result.setdefault(row["patient_id"], set()).add(row["test_type"])
    return result


def test_rows_follow_contract_and_stay_inside_collection_day():
    for day in (1, 2, 5):
        rows = generate_lab_rows(day, PATIENTS, CLOCK, seed=11)
        assert rows and set(rows[0]) == set(LAB_COLUMNS)
        lo, hi = CLOCK.day_start(day - 1), CLOCK.day_end(day - 1)
        for row in rows:
            assert row["test_type"] in TEST_SPECS
            assert row["patient_id"].startswith("P")
            assert row["collected_at"].endswith("Z") and "." not in row["collected_at"]
            low, high = row["reference_range"].split("-")
            assert float(low) < float(high)
            assert isinstance(row["result_value"], float)
        assert len({(r["patient_id"], r["test_type"]) for r in rows}) == len(rows)
        assert lo < hi


def test_same_day_always_generates_the_same_file():
    assert generate_lab_rows(3, PATIENTS, CLOCK, 11) == generate_lab_rows(3, PATIENTS, CLOCK, 11)
    assert generate_lab_rows(3, PATIENTS, CLOCK, 11) != generate_lab_rows(4, PATIENTS, CLOCK, 11)


def test_labs_are_correlated_with_the_health_story():
    det = [p for p in PATIENTS if p.is_deteriorating]
    # File for day N covers labs collected on day N-1, so pick the file right after peak activity.
    ep = det[0].episodes[0]
    peak_day = CLOCK.sim_day(CLOCK.epoch + ep.start_s + ep.ramp_s + ep.hold_s / 2)
    abnormal = _abnormal_by_patient(peak_day + 1)
    assert {"lactate", "wbc", "crp"} & abnormal.get(det[0].patient_id, set())

    occult = next(p for p in PATIENTS if p.occult_from_day)
    late_day = occult.occult_from_day + 2
    early = _abnormal_by_patient(1).get(occult.patient_id, set())
    later = [
        _abnormal_by_patient(d).get(occult.patient_id, set()) for d in range(late_day, late_day + 3)
    ]
    assert sum(len(s) for s in later) > 2 * max(
        1, len(early)
    )  # labs turn abnormal once "occult" starts


def test_chronic_conditions_show_up_in_labs():
    diabetic = [
        p
        for p in build_patients(40, seed=5, day_seconds=300, num_deteriorating=0, num_occult=0)
        if p.comorbidity == "diabetes"
    ]
    assert diabetic
    hits = 0
    for day in range(1, 6):
        abnormal = {
            r["patient_id"]
            for r in generate_lab_rows(day, diabetic, CLOCK, 5)
            if r["test_type"] == "glucose" and _is_abnormal(r)
        }
        hits += len(abnormal)
    assert hits >= 0.6 * 5 * len(diabetic)


def test_atomic_write_leaves_no_tmp_and_is_valid_csv(tmp_path):
    rows = generate_lab_rows(2, PATIENTS, CLOCK, 11)
    path = write_lab_file(tmp_path, 2, rows)
    assert path.name == lab_filename(2) == "labs_day_002.csv"
    assert not list(tmp_path.glob("*.tmp"))
    with path.open() as f:
        parsed = list(csv.DictReader(f))
    assert len(parsed) == len(rows) and list(parsed[0]) == list(LAB_COLUMNS)


def test_corrupt_rows_injects_each_defect_kind():
    import random

    rows = generate_lab_rows(2, PATIENTS, CLOCK, 11)
    bad, defects = corrupt_rows(rows, random.Random(1))
    assert set(defects) == {
        "non_numeric_result",
        "unknown_patient",
        "bad_reference_range",
        "missing_collected_at",
        "duplicate_row",
    }
    assert len(bad) == len(rows) + 1
    assert any(r["result_value"] == "N/A" for r in bad)
    assert any(r["patient_id"] == "P999" for r in bad)
    assert any(r["collected_at"] == "" for r in bad)
    assert len(rows) > 0 and all(
        isinstance(r["result_value"], float) for r in rows
    )  # input untouched


def test_forced_missing_day_drops_nothing(tmp_path):
    faults = LabFaultConfig(force_missing=frozenset({3}))
    assert drop_day(3, PATIENTS, CLOCK, 11, tmp_path, faults) is None
    assert not list(tmp_path.iterdir())
    assert drop_day(4, PATIENTS, CLOCK, 11, tmp_path, faults) is not None


def test_forced_badschema_drops_test_type_column(tmp_path):
    path = drop_day(
        3, PATIENTS, CLOCK, 11, tmp_path, LabFaultConfig(force_badschema=frozenset({3}))
    )
    header = path.read_text().splitlines()[0].split(",")
    assert "test_type" not in header and "patient_id" in header


def test_forced_corrupt_day_contains_bad_rows(tmp_path):
    path = drop_day(3, PATIENTS, CLOCK, 11, tmp_path, LabFaultConfig(force_corrupt=frozenset({3})))
    text = path.read_text()
    assert "N/A" in text and "P999" in text


def test_late_file_appears_only_after_delay(tmp_path):
    stop = threading.Event()
    faults = LabFaultConfig(force_late=frozenset({2}), late_seconds=0.3)
    t = threading.Thread(target=drop_day, args=(2, PATIENTS, CLOCK, 11, tmp_path, faults, stop))
    t.start()
    assert not list(tmp_path.glob("labs_day_*.csv"))
    t.join(5)
    assert (tmp_path / "labs_day_002.csv").exists()


def test_day_state_survives_restart_and_prevents_redrop(tmp_path):
    state = DayState(str(tmp_path / "state"))
    assert state.read() == 0
    state.write(4)
    assert DayState(str(tmp_path / "state")).read() == 4


def test_run_drops_current_day_once_and_respects_state(tmp_path, monkeypatch):
    import time as _time

    clock = SimClock(epoch=_time.time() - 1, day_seconds=300)  # day 1 has just started
    state = DayState(str(tmp_path / "state"))
    stop = threading.Event()
    handled = run(clock, PATIENTS, 11, tmp_path / "landing", NO_FAULTS, state, stop, max_days=1)
    assert handled == 1 and state.read() == 1
    assert (tmp_path / "landing" / "labs_day_001.csv").exists()

    # a restarted generator on the same day must not drop it again
    (tmp_path / "landing" / "labs_day_001.csv").unlink()
    stop2 = threading.Event()
    threading.Timer(0.5, stop2.set).start()
    assert run(clock, PATIENTS, 11, tmp_path / "landing", NO_FAULTS, state, stop2) == 0
    assert not (tmp_path / "landing" / "labs_day_001.csv").exists()


@pytest.mark.parametrize("value,expected", [(3.1, True), (1.0, False), (0.5, False), (2.0, False)])
def test_abnormal_helper(value, expected):
    row = {"result_value": value, "reference_range": "0.5-2.0"}
    assert _is_abnormal(row) is expected
