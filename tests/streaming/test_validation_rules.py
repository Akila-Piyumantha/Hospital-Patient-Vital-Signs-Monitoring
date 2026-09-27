"""Validation rules vs. what the simulator emits (no Spark needed)."""

import pytest

from simulators.faults import _GARBAGE
from simulators.vitals_model import LIMITS
from streaming.validation import VALID_RANGES, reason_for

GOOD = {
    "event_id": "e1",
    "patient_id": "P001",
    "heart_rate": 78,
    "spo2": 97,
    "systolic_bp": 121,
    "diastolic_bp": 79,
    "temperature": 36.8,
    "event_time": "2026-03-01T10:15:02.123Z",
    "sim_day": 3,
}


def test_every_physiological_extreme_of_the_simulator_is_accepted():
    for vital, (low, high) in LIMITS.items():
        v_low, v_high = VALID_RANGES[vital]
        assert v_low <= low and high <= v_high, vital


@pytest.mark.parametrize(("field", "value"), _GARBAGE)
def test_every_injected_garbage_value_is_rejected(field, value):
    record = {**GOOD, field: value}
    reason = reason_for(record)
    assert reason is not None
    assert reason.startswith("out_of_range") or reason == "implausible_bp"


@pytest.mark.parametrize(
    "field", ["heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature"]
)
def test_injected_nulls_are_rejected(field):
    assert reason_for({**GOOD, field: None}) == f"missing_field:{field}"


def test_rule_order_and_reasons():
    assert reason_for(GOOD) is None
    assert reason_for({**GOOD, "event_id": None}) == "malformed_record"
    assert reason_for({**GOOD, "event_time": None}) == "bad_timestamp"
    assert reason_for({**GOOD, "sim_day": None}) == "missing_field:sim_day"
    assert reason_for({**GOOD, "diastolic_bp": 121}) == "implausible_bp"
    assert reason_for(GOOD, known_patients={"P002"}) == "unknown_patient"
    assert reason_for(GOOD, known_patients={"P001"}) is None
