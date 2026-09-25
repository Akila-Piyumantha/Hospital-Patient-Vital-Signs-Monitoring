import pytest

from simulators.faults import FaultConfig, FaultInjector

EVENT = {
    "event_id": "e1", "patient_id": "P001", "heart_rate": 80, "spo2": 97,
    "systolic_bp": 120, "diastolic_bp": 78, "temperature": 36.8,
    "timestamp": "2026-01-01T00:00:00.000Z", "sim_day": 1,
}  # fmt: skip


def test_off_profile_is_a_pure_passthrough():
    inj = FaultInjector(FaultConfig.from_profile("off"))
    for i in range(500):
        assert inj.process(dict(EVENT), now=float(i)) == [EVENT]
    assert not inj.drain_faults()


def test_unknown_profile_rejected():
    with pytest.raises(ValueError):
        FaultConfig.from_profile("nope")


def test_env_override_beats_profile(monkeypatch):
    monkeypatch.setenv("FAULT_PROFILE", "off")
    monkeypatch.setenv("FAULT_NULL_RATE", "1.0")
    cfg = FaultConfig.from_env()
    assert cfg.null == 1.0 and cfg.garbage == 0.0


def test_null_fault_blanks_exactly_one_vital():
    inj = FaultInjector(FaultConfig(null=1.0), seed=1)
    (out,) = inj.process(dict(EVENT), now=0.0)
    assert (
        sum(
            out[k] is None
            for k in ("heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature")
        )
        == 1
    )
    assert inj.drain_faults()["null"] == 1


def test_garbage_fault_emits_physiologically_impossible_value():
    inj = FaultInjector(FaultConfig(garbage=1.0), seed=1)
    (out,) = inj.process(dict(EVENT), now=0.0)
    changed = {k: v for k, v in out.items() if EVENT[k] != v}
    assert len(changed) == 1 and inj.drain_faults()["garbage"] == 1
    ((field, value),) = changed.items()
    impossible = {"heart_rate": (0, 999), "spo2": (0, 150), "systolic_bp": (-5,),
                  "diastolic_bp": (400,), "temperature": (0.0, 99.9)}  # fmt: skip
    assert value in impossible[field]


def test_duplicate_reuses_event_id():
    inj = FaultInjector(FaultConfig(duplicate=1.0))
    out = inj.process(dict(EVENT), now=0.0)
    assert len(out) == 2 and out[0] == out[1]


def test_late_event_is_held_then_released_with_original_timestamp():
    inj = FaultInjector(FaultConfig(late=1.0, late_min_s=20, late_max_s=30), seed=2)
    assert inj.process(dict(EVENT), now=100.0) == []
    assert inj.pending_late == 1
    assert inj.release_due(now=110.0) == []  # too early
    (released,) = inj.release_due(now=131.0)
    assert released["timestamp"] == EVENT["timestamp"]  # stays "late" relative to now
    assert inj.pending_late == 0


def test_dropout_silences_only_that_patient_for_a_while():
    cfg = FaultConfig(dropout=1.0, dropout_min_s=10, dropout_max_s=10)
    inj = FaultInjector(cfg)
    assert inj.process(dict(EVENT), now=0.0) == []  # dropout starts
    assert inj.process(dict(EVENT), now=5.0) == []  # still silent
    other = dict(EVENT, patient_id="P002")
    inj2 = FaultInjector(FaultConfig(dropout=0.0))
    assert inj2.process(other, now=5.0) == [other]
    # after the outage the patient is heard again (rate now 0 so no re-trigger)
    inj.config = FaultConfig(dropout=0.0)
    assert inj.process(dict(EVENT), now=11.0) == [EVENT]


def test_chaos_profile_produces_dlq_worthy_traffic_within_expectation():
    inj = FaultInjector(FaultConfig.from_profile("chaos"), seed=4)
    bad = 0
    n = 5000
    for i in range(n):
        for e in inj.process(dict(EVENT, event_id=str(i)), now=float(i)):
            if (
                any(e[k] is None for k in ("heart_rate", "spo2"))
                or e["heart_rate"] in (0, 999)
                or e["spo2"] in (0, 150)
            ):
                bad += 1
    assert 0.03 * n < bad < 0.25 * n
