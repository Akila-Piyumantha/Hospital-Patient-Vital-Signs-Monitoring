"""Synthetic patient cohort and its scripted "ground truth" health story.

The cohort is a pure function of ``(n, seed, day_seconds)``, so the vitals
simulator, the lab generator and the DB seeding script all agree on who the
patients are and what happens to them without talking to each other.

Three kinds of patients make the business question answerable:

* **deteriorating** - a scripted episode (sepsis-like or respiratory) ramps vitals
  towards danger, holds, then recovers; a milder second episode follows later.
  Their labs turn abnormal while an episode is active.
* **occult** - vitals stay normal but labs turn abnormal from a given day on.
  These are the patients whose risk picture *only* changes once the daily lab
  file is joined in.
* **stable** - baseline noise, occasional random spikes (motion artefacts).

All times below are *seconds since the simulation epoch* unless stated otherwise.
Synthetic data only - no real patient information.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

COMORBIDITIES = ("none", "diabetes", "ckd", "anemia", "copd", "hypertension", "heart_failure")
_COMORBIDITY_WEIGHTS = (40, 15, 8, 8, 10, 14, 5)


def _smoothstep(u: float) -> float:
    u = min(1.0, max(0.0, u))
    return u * u * (3 - 2 * u)


@dataclass(frozen=True)
class Episode:
    """A scripted clinical deterioration: ramp up -> hold -> recover."""

    kind: str  # "sepsis" | "respiratory"
    start_s: float
    ramp_s: float
    hold_s: float
    recover_s: float
    severity: float = 1.0

    @property
    def end_s(self) -> float:
        return self.start_s + self.ramp_s + self.hold_s + self.recover_s

    def intensity(self, t: float) -> float:
        """0 when inactive, up to ``severity`` at the peak (smooth ramps)."""
        x = t - self.start_s
        if x <= 0:
            return 0.0
        if x < self.ramp_s:
            return self.severity * _smoothstep(x / self.ramp_s)
        if x < self.ramp_s + self.hold_s:
            return self.severity
        if x < self.end_s - self.start_s:
            return self.severity * (
                1 - _smoothstep((x - self.ramp_s - self.hold_s) / self.recover_s)
            )
        return 0.0


@dataclass(frozen=True)
class PatientProfile:
    patient_id: str
    name: str
    age: int
    sex: str
    ward: str
    bed: str
    comorbidity: str
    baseline_hr: int
    baseline_spo2: int
    baseline_sbp: int
    baseline_dbp: int
    baseline_temp: float
    episodes: tuple[Episode, ...] = ()
    occult_from_day: int | None = None  # labs abnormal from this sim day on, vitals normal

    @property
    def comorbidity_flag(self) -> bool:
        return self.comorbidity != "none"

    @property
    def is_deteriorating(self) -> bool:
        return bool(self.episodes)

    def active_episode(self, t: float) -> tuple[Episode | None, float]:
        """The episode with the highest intensity at ``t`` and that intensity."""
        best: tuple[Episode | None, float] = (None, 0.0)
        for episode in self.episodes:
            value = episode.intensity(t)
            if value > best[1]:
                best = (episode, value)
        return best

    def peak_intensity(
        self, t0: float, t1: float, samples: int = 24
    ) -> tuple[Episode | None, float]:
        """Strongest episode activity over ``[t0, t1]`` (drives that day's lab abnormality)."""
        best: tuple[Episode | None, float] = (None, 0.0)
        for i in range(samples + 1):
            episode, value = self.active_episode(t0 + (t1 - t0) * i / samples)
            if value > best[1]:
                best = (episode, value)
        return best

    def is_occult_on(self, sim_day: int) -> bool:
        return self.occult_from_day is not None and sim_day >= self.occult_from_day


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def build_patients(
    n: int = 20,
    seed: int = 42,
    day_seconds: float = 300.0,
    num_deteriorating: int = 4,
    num_occult: int = 2,
) -> list[PatientProfile]:
    """Deterministically build the cohort ``P001..P0nn``."""
    if num_deteriorating + num_occult > n:
        raise ValueError("num_deteriorating + num_occult must not exceed the cohort size")

    rng = random.Random(seed)
    ids = [f"P{i:03d}" for i in range(1, n + 1)]
    deteriorating = sorted(rng.sample(ids, num_deteriorating))
    occult = set(rng.sample([i for i in ids if i not in deteriorating], num_occult))

    patients: list[PatientProfile] = []
    for index, patient_id in enumerate(ids):
        is_det = patient_id in deteriorating
        is_occult = patient_id in occult
        # Keep scripted patients free of chronic conditions so their story is unambiguous.
        comorbidity = (
            "none"
            if is_det or is_occult
            else rng.choices(COMORBIDITIES, weights=_COMORBIDITY_WEIGHTS)[0]
        )
        age = int(_clip(rng.gauss(58, 16), 19, 92))

        if comorbidity == "copd":
            spo2 = round(_clip(rng.gauss(94, 0.8), 92, 96))
        else:
            spo2 = round(_clip(rng.gauss(97.5, 0.8), 95, 99))
        if comorbidity in ("hypertension", "heart_failure"):
            sbp = round(_clip(rng.gauss(138, 7), 128, 150))
        else:
            sbp = round(_clip(rng.gauss(119, 8), 104, 134))
        dbp = round(_clip(sbp * 0.63 + rng.gauss(0, 3), 58, 92))
        hr = round(_clip(rng.gauss(74, 7), 58, 90))
        temp = round(_clip(rng.gauss(36.7, 0.2), 36.2, 37.1), 1)

        episodes: tuple[Episode, ...] = ()
        if is_det:
            k = deteriorating.index(patient_id)
            kind = "sepsis" if k % 2 == 0 else "respiratory"
            start = (1.0 + 0.8 * k + rng.uniform(0.0, 0.3)) * day_seconds
            built: list[Episode] = []
            for repeat in range(2):
                hold = rng.uniform(0.8, 1.4) * day_seconds
                episode = Episode(
                    kind=kind,
                    start_s=start,
                    ramp_s=0.8 * day_seconds,
                    hold_s=hold,
                    recover_s=0.6 * day_seconds,
                    severity=1.0 if repeat == 0 else 0.7,
                )
                built.append(episode)
                start = episode.end_s + 1.0 * day_seconds
            episodes = tuple(built)

        patients.append(
            PatientProfile(
                patient_id=patient_id,
                name=f"Synthetic Patient {patient_id[1:]}",
                age=age,
                sex=rng.choice("MF"),
                ward="Ward-A",
                bed=f"A-{index + 1:02d}",
                comorbidity=comorbidity,
                baseline_hr=hr,
                baseline_spo2=spo2,
                baseline_sbp=sbp,
                baseline_dbp=dbp,
                baseline_temp=temp,
                episodes=episodes,
                occult_from_day=rng.randint(2, 3) if is_occult else None,
            )
        )
    return patients
