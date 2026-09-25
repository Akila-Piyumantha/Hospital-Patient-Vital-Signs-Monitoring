"""Physiological model behind the bedside-monitor readings.

reading(t) = baseline + episode delta + slowly-varying noise + transient spike

* **noise** is an AR(1) process per vital (mean-reverting, so readings wander
  realistically instead of jumping independently each tick);
* **episode delta** follows the scripted deterioration (see ``patients.py``);
* **spikes** are short (1-3 readings) random excursions, like a motion artefact
  or a genuine acute event - the "occasional simulated abnormal spikes" of the brief.

Everything is driven by per-patient seeded RNGs, so a run is reproducible for a
given ``SIM_SEED`` and independent of the order patients are processed in.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

from simulators.patients import PatientProfile

# Physical limits of what a sensor could plausibly report *before* fault injection.
LIMITS = {
    "heart_rate": (30, 220),
    "spo2": (70, 100),
    "systolic_bp": (60, 240),
    "diastolic_bp": (30, 140),
    "temperature": (33.0, 41.0),
}

# (noise sigma, AR(1) coefficient) per vital
_NOISE = {
    "heart_rate": (2.5, 0.92),
    "spo2": (0.5, 0.90),
    "systolic_bp": (3.0, 0.93),
    "diastolic_bp": (2.0, 0.93),
    "temperature": (0.06, 0.97),
}

# Vital deltas at full episode intensity (severity 1.0)
_EPISODE_DELTAS = {
    "sepsis": {
        "heart_rate": 45.0,
        "spo2": -4.0,
        "systolic_bp": -35.0,
        "diastolic_bp": -20.0,
        "temperature": 2.3,
    },
    "respiratory": {
        "heart_rate": 30.0,
        "spo2": -11.0,
        "systolic_bp": 10.0,
        "diastolic_bp": 5.0,
        "temperature": 0.5,
    },
}

# Random spike: vital -> (min delta, max delta); sign chosen per spike where listed twice.
_SPIKES = (
    ("heart_rate", 40.0, 70.0),
    ("spo2", -15.0, -8.0),
    ("systolic_bp", 40.0, 60.0),
    ("systolic_bp", -55.0, -35.0),
    ("temperature", 1.5, 2.5),
)


@dataclass
class _PatientState:
    rng: random.Random
    noise: dict[str, float] = field(default_factory=lambda: dict.fromkeys(LIMITS, 0.0))
    spike_left: int = 0
    spike_vital: str = ""
    spike_delta: float = 0.0


class VitalsSimulator:
    """Generates the next reading for a patient at ``t`` seconds since the epoch."""

    def __init__(self, patients: list[PatientProfile], seed: int, spike_rate: float = 0.01) -> None:
        self.spike_rate = spike_rate
        self._state = {
            p.patient_id: _PatientState(random.Random(f"{seed}:{p.patient_id}")) for p in patients
        }

    def reading(self, patient: PatientProfile, t: float) -> dict[str, float | int]:
        state = self._state[patient.patient_id]
        rng = state.rng

        episode, intensity = patient.active_episode(t)
        deltas = _EPISODE_DELTAS[episode.kind] if episode else {}

        # Advance the transient spike state machine.
        if state.spike_left > 0:
            state.spike_left -= 1
        elif rng.random() < self.spike_rate:
            vital, low, high = rng.choice(_SPIKES)
            state.spike_vital = vital
            state.spike_delta = rng.uniform(low, high)
            state.spike_left = rng.randint(1, 3)
        spike_active = state.spike_left > 0

        baselines = {
            "heart_rate": patient.baseline_hr,
            "spo2": patient.baseline_spo2,
            "systolic_bp": patient.baseline_sbp,
            "diastolic_bp": patient.baseline_dbp,
            "temperature": patient.baseline_temp,
        }
        values: dict[str, float] = {}
        for vital, base in baselines.items():
            sigma, phi = _NOISE[vital]
            state.noise[vital] = phi * state.noise[vital] + sigma * math.sqrt(
                1 - phi**2
            ) * rng.gauss(0, 1)
            value = base + state.noise[vital] + deltas.get(vital, 0.0) * intensity
            if spike_active and vital == state.spike_vital:
                value += state.spike_delta
            low_limit, high_limit = LIMITS[vital]
            values[vital] = min(high_limit, max(low_limit, value))

        # Physiologically the diastolic pressure stays below the systolic one.
        values["diastolic_bp"] = min(values["diastolic_bp"], values["systolic_bp"] - 15)

        return {
            "heart_rate": round(values["heart_rate"]),
            "spo2": round(values["spo2"]),
            "systolic_bp": round(values["systolic_bp"]),
            "diastolic_bp": round(values["diastolic_bp"]),
            "temperature": round(values["temperature"], 1),
        }
