import statistics

import pytest

from simulators.patients import Episode, build_patients
from simulators.vitals_model import LIMITS, VitalsSimulator

DAY = 300.0


def test_cohort_is_deterministic_and_shaped_as_requested():
    a = build_patients(20, seed=7, day_seconds=DAY)
    b = build_patients(20, seed=7, day_seconds=DAY)
    assert a == b
    assert [p.patient_id for p in a] == [f"P{i:03d}" for i in range(1, 21)]
    assert sum(p.is_deteriorating for p in a) == 4
    assert sum(p.occult_from_day is not None for p in a) == 2
    assert not any(p.is_deteriorating and p.occult_from_day for p in a)


def test_different_seed_gives_different_cohort():
    assert build_patients(20, seed=1) != build_patients(20, seed=2)


def test_too_many_scripted_patients_rejected():
    with pytest.raises(ValueError):
        build_patients(5, num_deteriorating=4, num_occult=2)


def test_episode_intensity_shape():
    ep = Episode("sepsis", start_s=100, ramp_s=100, hold_s=100, recover_s=100, severity=0.8)
    assert ep.intensity(50) == 0
    assert 0 < ep.intensity(150) < 0.8  # ramping
    assert ep.intensity(250) == pytest.approx(0.8)  # holding at severity
    assert 0 < ep.intensity(350) < 0.8  # recovering
    assert ep.intensity(500) == 0
    assert ep.end_s == 400


def test_baseline_readings_stay_physiological_and_diastolic_below_systolic():
    patients = build_patients(20, seed=3, day_seconds=DAY)
    stable = [p for p in patients if not p.is_deteriorating]
    sim = VitalsSimulator(stable, seed=3, spike_rate=0.0)
    for _ in range(300):
        for p in stable:
            r = sim.reading(p, t=0.0)  # t=0: before any episode has started
            for vital, (low, high) in LIMITS.items():
                assert low <= r[vital] <= high
            assert r["diastolic_bp"] < r["systolic_bp"]


def test_deterioration_episode_moves_vitals_the_right_way():
    p = next(p for p in build_patients(20, seed=3, day_seconds=DAY) if p.episodes)
    ep = p.episodes[0]
    peak_t = ep.start_s + ep.ramp_s + ep.hold_s / 2

    def average(t0, n=60):
        sim = VitalsSimulator([p], seed=3, spike_rate=0.0)
        rows = [sim.reading(p, t0 + i * 0.1) for i in range(n)]
        return {k: statistics.mean(r[k] for r in rows) for k in rows[0]}

    calm, peak = average(0.0), average(peak_t)
    assert peak["heart_rate"] > calm["heart_rate"] + 15
    assert peak["spo2"] < calm["spo2"] - 1.5
    if ep.kind == "sepsis":
        assert peak["systolic_bp"] < calm["systolic_bp"] - 12
        assert peak["temperature"] > calm["temperature"] + 0.8


def test_spikes_occur_at_configured_rate_and_are_short():
    p = build_patients(4, seed=5, day_seconds=DAY, num_deteriorating=0, num_occult=0)[0]
    quiet = VitalsSimulator([p], seed=5, spike_rate=0.0)
    spiky = VitalsSimulator([p], seed=5, spike_rate=0.5)
    baseline = [quiet.reading(p, 0.0) for _ in range(400)]
    spiked = [spiky.reading(p, 0.0) for _ in range(400)]

    def excursions(rows):
        return sum(
            r["heart_rate"] > p.baseline_hr + 30
            or r["spo2"] < p.baseline_spo2 - 6
            or r["systolic_bp"] > p.baseline_sbp + 30
            or r["systolic_bp"] < p.baseline_sbp - 30
            or r["temperature"] > p.baseline_temp + 1.2
            for r in rows
        )

    assert excursions(baseline) == 0
    assert (
        excursions(spiked) > 20
    )  # rate 0.5 -> plenty of spikes; no spike is longer than 3 readings


def test_model_is_reproducible_for_a_seed():
    ps = build_patients(6, seed=9, day_seconds=DAY, num_deteriorating=1, num_occult=1)
    run = lambda: [  # noqa: E731
        VitalsSimulator(ps, seed=9).reading(p, t) for p in ps for t in (0.0, 2.0, 400.0)
    ]
    assert run() == run()
