"""Fault injection for the vitals stream.

Real bedside monitors misbehave, and a pipeline that only ever sees clean data
proves nothing. This module deliberately damages the stream so each robustness
feature downstream (validation -> DLQ, de-duplication, watermarks, freshness
alerts) has something to prove itself against:

=========  ================================================  ===========================
fault      what is emitted                                   exercised downstream
=========  ================================================  ===========================
null       one vital is ``null``                             validation -> DLQ
garbage    impossible value (HR 0/999, SpO2 150, ...)        validation -> DLQ
duplicate  same ``event_id`` sent twice                      dropDuplicates
late       held back 20-90 s, original timestamp kept        watermark / batch reconcile
dropout    sensor silent for 10-40 s for one patient         stale-patient detection
=========  ================================================  ===========================

Profiles (``FAULT_PROFILE``): ``off`` | ``low`` (default; ~2 % DLQ traffic) |
``chaos`` (~16 % DLQ traffic, enough to trip the ``DlqRateHigh`` alert).
Any single rate can be overridden with ``FAULT_<KIND>_RATE``.
"""

from __future__ import annotations

import heapq
import random
from collections import Counter
from dataclasses import dataclass

from common.config import env_float, env_str

FAULT_KINDS = ("null", "garbage", "duplicate", "late", "dropout")

_PROFILES: dict[str, dict[str, float]] = {
    "off": dict.fromkeys(FAULT_KINDS, 0.0),
    "low": {"null": 0.01, "garbage": 0.01, "duplicate": 0.01, "late": 0.02, "dropout": 0.002},
    "chaos": {"null": 0.08, "garbage": 0.08, "duplicate": 0.05, "late": 0.10, "dropout": 0.01},
}

_GARBAGE = (
    ("heart_rate", 0),
    ("heart_rate", 999),
    ("spo2", 0),
    ("spo2", 150),
    ("systolic_bp", -5),
    ("diastolic_bp", 400),
    ("temperature", 0.0),
    ("temperature", 99.9),
)
_NULLABLE = ("heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature")


@dataclass(frozen=True)
class FaultConfig:
    null: float = 0.0
    garbage: float = 0.0
    duplicate: float = 0.0
    late: float = 0.0
    dropout: float = 0.0
    late_min_s: float = 20.0
    late_max_s: float = 90.0
    dropout_min_s: float = 10.0
    dropout_max_s: float = 40.0

    @classmethod
    def from_profile(cls, profile: str) -> FaultConfig:
        if profile not in _PROFILES:
            raise ValueError(f"unknown FAULT_PROFILE {profile!r}; choose from {sorted(_PROFILES)}")
        return cls(**_PROFILES[profile])

    @classmethod
    def from_env(cls) -> FaultConfig:
        profile = env_str("FAULT_PROFILE", "low")
        if profile not in _PROFILES:
            raise ValueError(f"unknown FAULT_PROFILE {profile!r}; choose from {sorted(_PROFILES)}")
        base = _PROFILES[profile]
        rates = {kind: env_float(f"FAULT_{kind.upper()}_RATE", base[kind]) for kind in FAULT_KINDS}
        return cls(**rates)


class FaultInjector:
    """Stateful wrapper: ``process(event, now)`` returns the events to actually emit."""

    def __init__(self, config: FaultConfig, seed: int = 0) -> None:
        self.config = config
        self._rng = random.Random(f"faults:{seed}")
        self._dropped_until: dict[str, float] = {}
        self._late_heap: list[tuple[float, int, dict]] = []
        self._late_counter = 0
        self._faults: Counter[str] = Counter()

    # -- public API -----------------------------------------------------------------
    def process(self, event: dict, now: float) -> list[dict]:
        """Apply faults to one freshly generated event; returns 0..2 events to emit now."""
        cfg = self.config
        patient = event["patient_id"]

        # Sensor dropout: emit nothing while the patient's monitor is "disconnected".
        if now < self._dropped_until.get(patient, 0.0):
            return []
        if self._hit(cfg.dropout):
            duration = self._rng.uniform(cfg.dropout_min_s, cfg.dropout_max_s)
            self._dropped_until[patient] = now + duration
            self._faults["dropout"] += 1
            return []

        event = dict(event)
        if self._hit(cfg.null):
            event[self._rng.choice(_NULLABLE)] = None
            self._faults["null"] += 1
        elif self._hit(cfg.garbage):
            field, value = self._rng.choice(_GARBAGE)
            event[field] = value
            self._faults["garbage"] += 1

        if self._hit(cfg.late):
            release_at = now + self._rng.uniform(cfg.late_min_s, cfg.late_max_s)
            self._late_counter += 1
            heapq.heappush(self._late_heap, (release_at, self._late_counter, event))
            self._faults["late"] += 1
            return []

        out = [event]
        if self._hit(cfg.duplicate):
            out.append(dict(event))
            self._faults["duplicate"] += 1
        return out

    def release_due(self, now: float) -> list[dict]:
        """Late events whose delay has elapsed (original timestamps are preserved)."""
        due = []
        while self._late_heap and self._late_heap[0][0] <= now:
            due.append(heapq.heappop(self._late_heap)[2])
        return due

    def drain_faults(self) -> Counter[str]:
        """Faults injected since the last call (for metrics/logging)."""
        drained, self._faults = self._faults, Counter()
        return drained

    @property
    def pending_late(self) -> int:
        return len(self._late_heap)

    # -- internals ------------------------------------------------------------------
    def _hit(self, probability: float) -> bool:
        return probability > 0 and self._rng.random() < probability
