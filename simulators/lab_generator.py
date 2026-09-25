"""Batch source: the pathology lab's daily results file.

At the start of every simulated day *N* one CSV ``labs_day_NNN.csv`` is dropped into
the landing zone. It holds the labs **collected during day N-1** ("yesterday's
results"). Day 1's file is a baseline collected on a virtual day 0, dropped at start-up
so the first demo minutes already have lab context.

Contract (PROJECT_PLAN.md 4.2)::

    patient_id,test_type,result_value,reference_range,collected_at
    P001,lactate,3.1,0.5-2.0,2026-03-01T08:30:00Z

Realism that makes the business question answerable:

* deteriorating patients get abnormal labs (lactate, WBC, CRP, creatinine) while an
  episode is active - vitals and labs agree;
* *occult* patients keep normal vitals but abnormal labs from a given day on - their
  risk only shows up once the lab file is joined in;
* chronic conditions (diabetes -> glucose, CKD -> creatinine, anaemia -> haemoglobin)
  produce standing abnormalities; a few random abnormal results add noise.

Robustness testing (all off by default; see README): ``LAB_FORCE_MISSING_DAYS``,
``LAB_FORCE_LATE_DAYS``, ``LAB_FORCE_CORRUPT_DAYS`` (bad rows) and
``LAB_FORCE_BADSCHEMA_DAYS`` (wrong header) make failure demos deterministic;
``LAB_*_RATE`` do the same randomly. Files are written to ``*.tmp`` and renamed, so a
consumer globbing ``labs_day_*.csv`` never sees a half-written file.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import signal
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from common.config import Settings, env_float, env_int_set
from common.logging_setup import configure_logging, get_logger
from common.schemas import LAB_COLUMNS
from common.sim_clock import SimClock, iso_utc, load_clock
from simulators import lab_metrics as metrics
from simulators.patients import PatientProfile, build_patients

log = get_logger("lab_generator", "ingestion")


@dataclass(frozen=True)
class TestSpec:
    low: float
    high: float
    decimals: int
    abn_scale: float = 1.0  # how far outside the range abnormal results typically land

    @property
    def span(self) -> float:
        return self.high - self.low

    @property
    def reference_range(self) -> str:
        return f"{self.low:g}-{self.high:g}"


TEST_SPECS: dict[str, TestSpec] = {
    "potassium": TestSpec(3.5, 5.0, 1),  # mmol/L
    "creatinine": TestSpec(0.6, 1.3, 2),  # mg/dL
    "lactate": TestSpec(0.5, 2.0, 1),  # mmol/L
    "wbc": TestSpec(4.0, 11.0, 1),  # 10^9/L
    "crp": TestSpec(0.0, 5.0, 1, abn_scale=8.0),  # mg/L
    "hemoglobin": TestSpec(12.0, 17.5, 1),  # g/dL
    "glucose": TestSpec(70.0, 140.0, 0),  # mg/dL
}

# Direction(s) an abnormal result can take.
_DIRECTIONS = {
    "potassium": ("high", "low"),
    "creatinine": ("high",),
    "lactate": ("high",),
    "wbc": ("high",),
    "crp": ("high",),
    "hemoglobin": ("low",),
    "glucose": ("high", "low"),
}

# Per-test abnormality probability at full episode intensity.
_EPISODE_WEIGHTS = {
    "sepsis": {"lactate": 0.95, "wbc": 0.90, "crp": 0.90, "creatinine": 0.60, "glucose": 0.30},
    "respiratory": {"lactate": 0.50, "crp": 0.55, "wbc": 0.35, "potassium": 0.15},
}

# Chronic condition -> {test: probability of being abnormal on any given day}
_CHRONIC = {
    "diabetes": {"glucose": 0.85},
    "ckd": {"creatinine": 0.90, "potassium": 0.40},
    "anemia": {"hemoglobin": 0.90},
    "heart_failure": {"creatinine": 0.35, "potassium": 0.25},
}

_OCCULT_TESTS = {"lactate": 0.90, "wbc": 0.85, "creatinine": 0.75, "crp": 0.60}
_BASE_NOISE = 0.04  # chance any test is randomly abnormal
_TEST_PROBABILITY = 0.85  # chance a patient has a given test ordered on a given day


def _draw_value(rng: random.Random, name: str, abnormal: bool, severity: float) -> float:
    spec = TEST_SPECS[name]
    if not abnormal:
        mid = (spec.low + spec.high) / 2
        value = rng.gauss(mid, spec.span / 8)
        value = min(spec.high - 0.02 * spec.span, max(spec.low + 0.02 * spec.span, value))
    else:
        direction = rng.choice(_DIRECTIONS[name])
        push = rng.uniform(0.2, 1.0) * (0.5 + severity) * spec.abn_scale * spec.span * 0.5
        value = spec.high + push if direction == "high" else max(0.0, spec.low - push * 0.6)
        if name == "crp":
            value = spec.high + push * 2.5
    return round(value, spec.decimals)


def abnormal_probabilities(
    patient: PatientProfile, clock: SimClock, file_day: int
) -> dict[str, float]:
    """Per-test probability of an abnormal result in the file for ``file_day``."""
    window_start = clock.day_start(file_day - 1) - clock.epoch
    window_end = clock.day_end(file_day - 1) - clock.epoch
    probabilities = dict.fromkeys(TEST_SPECS, _BASE_NOISE)

    def raise_to(test: str, p: float) -> None:
        probabilities[test] = max(probabilities[test], min(1.0, p))

    episode, intensity = patient.peak_intensity(window_start, window_end)
    if episode:
        for test, weight in _EPISODE_WEIGHTS[episode.kind].items():
            raise_to(test, weight * intensity)
    for test, p in _CHRONIC.get(patient.comorbidity, {}).items():
        raise_to(test, p)
    if patient.is_occult_on(file_day - 1):
        for test, p in _OCCULT_TESTS.items():
            raise_to(test, p)
    return probabilities


def generate_lab_rows(
    file_day: int, patients: list[PatientProfile], clock: SimClock, seed: int
) -> list[dict[str, str | float]]:
    """All rows of ``labs_day_<file_day>.csv`` (labs collected on sim day ``file_day - 1``)."""
    rng = random.Random(f"labs:{seed}:{file_day}")  # same file regardless of restarts
    window_start = clock.day_start(file_day - 1)
    window_end = clock.day_end(file_day - 1)
    rows: list[dict[str, str | float]] = []
    for patient in patients:
        probabilities = abnormal_probabilities(patient, clock, file_day)
        _, severity = patient.peak_intensity(window_start - clock.epoch, window_end - clock.epoch)
        collected = window_start + rng.uniform(0.05, 0.35) * clock.day_seconds
        for test, spec in TEST_SPECS.items():
            if rng.random() > _TEST_PROBABILITY and probabilities[test] <= _BASE_NOISE:
                continue  # test not ordered today
            abnormal = rng.random() < probabilities[test]
            rows.append(
                {
                    "patient_id": patient.patient_id,
                    "test_type": test,
                    "result_value": _draw_value(rng, test, abnormal, severity),
                    "reference_range": spec.reference_range,
                    # contract example has whole-second precision: 2026-03-01T08:30:00Z
                    "collected_at": iso_utc(collected + rng.uniform(0, 0.05 * clock.day_seconds))[
                        :19
                    ]
                    + "Z",
                }
            )
    return rows


def corrupt_rows(rows: list[dict], rng: random.Random) -> tuple[list[dict], list[str]]:
    """Inject row-level defects a validating consumer must catch. Returns (rows, defects)."""
    rows = [dict(row) for row in rows]
    defects: list[str] = []
    if len(rows) < 6:
        return rows, defects
    picks = rng.sample(range(len(rows)), 4)
    rows[picks[0]]["result_value"] = "N/A"
    defects.append("non_numeric_result")
    rows[picks[1]]["patient_id"] = "P999"
    defects.append("unknown_patient")
    rows[picks[2]]["reference_range"] = "??"
    defects.append("bad_reference_range")
    rows[picks[3]]["collected_at"] = ""
    defects.append("missing_collected_at")
    rows.append(dict(rows[rng.randrange(len(rows) - 1)]))
    defects.append("duplicate_row")
    return rows, defects


def lab_filename(day: int) -> str:
    return f"labs_day_{day:03d}.csv"


def write_lab_file(directory: Path, day: int, rows: list[dict], columns=LAB_COLUMNS) -> Path:
    """Atomic drop: write ``*.tmp``, fsync, rename to the final name."""
    directory.mkdir(parents=True, exist_ok=True)
    final = directory / lab_filename(day)
    tmp = final.with_suffix(".csv.tmp")
    with tmp.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, final)
    return final


@dataclass(frozen=True)
class LabFaultConfig:
    missing_rate: float = 0.0
    late_rate: float = 0.0
    corrupt_rate: float = 0.0
    force_missing: frozenset[int] = frozenset()
    force_late: frozenset[int] = frozenset()
    force_corrupt: frozenset[int] = frozenset()
    force_badschema: frozenset[int] = frozenset()
    late_seconds: float = 90.0

    @classmethod
    def from_env(cls) -> LabFaultConfig:
        return cls(
            missing_rate=env_float("LAB_MISSING_RATE", 0.0),
            late_rate=env_float("LAB_LATE_RATE", 0.0),
            corrupt_rate=env_float("LAB_CORRUPT_RATE", 0.0),
            force_missing=env_int_set("LAB_FORCE_MISSING_DAYS"),
            force_late=env_int_set("LAB_FORCE_LATE_DAYS"),
            force_corrupt=env_int_set("LAB_FORCE_CORRUPT_DAYS"),
            force_badschema=env_int_set("LAB_FORCE_BADSCHEMA_DAYS"),
            late_seconds=env_float("LAB_LATE_SECONDS", 90.0),
        )


def drop_day(
    day: int,
    patients: list[PatientProfile],
    clock: SimClock,
    seed: int,
    directory: Path,
    faults: LabFaultConfig,
    stop: threading.Event | None = None,
) -> Path | None:
    """Produce (or deliberately fail to produce) the file for ``day``."""
    rng = random.Random(f"lab-faults:{seed}:{day}")
    ctx = {"sim_day": day, "collected_sim_day": day - 1}

    if day in faults.force_missing or rng.random() < faults.missing_rate:
        metrics.LAB_FILES_SKIPPED.labels(reason="missing").inc()
        log.error("lab_file_not_dropped", reason="simulated_missing_file", **ctx)
        return None

    rows = generate_lab_rows(day, patients, clock, seed)
    columns: tuple[str, ...] = LAB_COLUMNS
    defects: list[str] = []
    if day in faults.force_badschema:
        columns = tuple(c for c in LAB_COLUMNS if c != "test_type")
        defects.append("missing_column_test_type")
    elif day in faults.force_corrupt or rng.random() < faults.corrupt_rate:
        rows, defects = corrupt_rows(rows, rng)
    for defect in defects:
        metrics.LAB_FILES_FAULTY.labels(kind=defect).inc()

    if day in faults.force_late or rng.random() < faults.late_rate:
        log.warning("lab_file_delayed", delay_s=faults.late_seconds, **ctx)
        metrics.LAB_FILES_FAULTY.labels(kind="late").inc()
        if stop is not None:
            stop.wait(faults.late_seconds)
        else:
            time.sleep(faults.late_seconds)

    path = write_lab_file(directory, day, rows, columns)
    metrics.LAB_FILES_DROPPED.inc()
    metrics.LAB_ROWS_GENERATED.inc(len(rows))
    abnormal = sum(1 for r in rows if _is_abnormal(r))
    log.info(
        "lab_file_dropped",
        file=str(path),
        rows=len(rows),
        abnormal_rows=abnormal,
        defects=defects,
        **ctx,
    )
    return path


def _is_abnormal(row: dict) -> bool:
    try:
        low, high = (float(x) for x in str(row["reference_range"]).split("-"))
        return not low <= float(row["result_value"]) <= high
    except (ValueError, KeyError):
        return False


class DayState:
    """Remembers the last handled day so restarts do not re-drop old files."""

    def __init__(self, directory: str) -> None:
        self.path = Path(directory) / "lab_generator.json"

    def read(self) -> int:
        try:
            return int(json.loads(self.path.read_text())["last_day"])
        except (OSError, ValueError, KeyError):
            return 0

    def write(self, day: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"last_day": day}))


def run(
    clock: SimClock,
    patients: list[PatientProfile],
    seed: int,
    directory: Path,
    faults: LabFaultConfig,
    state: DayState,
    stop: threading.Event,
    max_days: int | None = None,
) -> int:
    """Drop one file per simulated day, at the day boundary. Returns files handled."""
    handled = 0
    metrics.LAB_LAST_DAY.set(state.read())  # survive restarts: show the real last day
    while not stop.is_set():
        now = time.time()
        day = clock.sim_day(now)
        last = state.read()
        if day > last and day >= 1:
            # After a long outage only the current day is dropped (no flood of stale files).
            drop_day(day, patients, clock, seed, directory, faults, stop)
            state.write(day)
            metrics.LAB_LAST_DAY.set(day)
            handled += 1
            if max_days is not None and handled >= max_days:
                break
        stop.wait(clock.seconds_until_next_day() + 0.25)
    return handled


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily lab-results batch source")
    parser.add_argument("--once", type=int, metavar="DAY", help="write the file for DAY and exit")
    parser.add_argument("--out-dir", default=None, help="override LANDING_DIR")
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    configure_logging("lab-generator", settings.log_level)
    clock = load_clock(settings.sim_day_seconds, settings.sim_epoch, settings.sim_epoch_file)
    patients = build_patients(
        settings.num_patients,
        settings.sim_seed,
        settings.sim_day_seconds,
        settings.num_deteriorating,
        settings.num_occult,
    )
    directory = Path(args.out_dir or settings.landing_dir)
    faults = LabFaultConfig.from_env()

    if args.once is not None:
        return (
            0 if drop_day(args.once, patients, clock, settings.sim_seed, directory, faults) else 1
        )

    from prometheus_client import start_http_server

    start_http_server(int(env_float("METRICS_PORT", 8002)))
    log.info(
        "lab_generator_started",
        landing_dir=str(directory),
        sim_epoch=iso_utc(clock.epoch),
        current_sim_day=clock.sim_day(),
        faults=faults.__dict__,
    )
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    run(clock, patients, settings.sim_seed, directory, faults, DayState(settings.state_dir), stop)
    log.info("lab_generator_stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
