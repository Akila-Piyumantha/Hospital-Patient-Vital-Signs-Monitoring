"""Daily lab file: locate, validate, load, lab risk, archive (tasks C3, contract 4.2 / 4.5).

Validation happens at two levels:

* **file** - unreadable file or a header without the contract columns: the whole file moves to
  ``quarantine/`` with a ``.reason.txt`` next to it; nothing is loaded and the previous lab risk
  stays in force.
* **row** - unknown patient, unknown test, non-numeric / negative result, bad reference range,
  bad ``collected_at``, duplicates: the row is dropped and written to
  ``quarantine/labs_day_NNN.rejected.csv`` with its reason; the rest of the file is loaded.

Pure functions (``validate_rows``, ``lab_risk_by_patient``) carry the logic and are unit-tested;
the ``load_*`` / ``write_*`` functions are thin, idempotent database writes (a day is replaced as
a whole in one transaction, so re-running it gives the same rows).
"""

from __future__ import annotations

import csv
import io
import math
import shutil
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common import scoring
from common.schemas import LAB_COLUMNS

KNOWN_TESTS = frozenset(scoring.LAB_POINTS)


def lab_filename(day: int) -> str:
    return f"labs_day_{day:03d}.csv"


@dataclass(frozen=True)
class LabResult:
    patient_id: str
    test_type: str
    result_value: float
    ref_low: float
    ref_high: float
    abnormal_flag: str | None  # "HIGH" / "LOW" / None
    collected_at: datetime

    @property
    def direction(self) -> str | None:
        return self.abnormal_flag.lower() if self.abnormal_flag else None


@dataclass
class ValidationResult:
    rows: list[LabResult] = field(default_factory=list)
    rejected: list[tuple[dict[str, Any], str]] = field(default_factory=list)
    file_error: str | None = None
    rows_in: int = 0

    @property
    def reject_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for _, reason in self.rejected:
            counts[reason] = counts.get(reason, 0) + 1
        return counts


# --------------------------------------------------------------------------- locate / move
def find_lab_file(landing: Path, day: int) -> tuple[Path, str] | None:
    """Where the file for ``day`` is: ``landing`` (new), ``processed`` (replay), ``quarantine``."""
    name = lab_filename(day)
    for place, path in (
        ("landing", landing / name),
        ("processed", landing / "processed" / name),
        ("quarantine", landing / "quarantine" / name),
    ):
        if path.is_file():
            return path, place
    return None


def move(path: Path, directory: Path) -> Path:
    """Move ``path`` into ``directory`` (replacing an older copy); returns the new path."""
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / path.name
    if path.resolve() != target.resolve():
        shutil.move(str(path), str(target))
    return target


# ------------------------------------------------------------------------------ validation
def parse_timestamp(text: str) -> datetime:
    value = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def validate_row(row: Mapping[str, Any], known: set[str]) -> LabResult | str:
    """A valid ``LabResult`` or the rejection reason."""
    patient = (row.get("patient_id") or "").strip()
    if not patient or patient not in known:
        return "unknown_patient"
    test = (row.get("test_type") or "").strip().lower()
    if test not in KNOWN_TESTS:
        return "unknown_test_type"
    try:
        value = float(row.get("result_value") or "")
    except ValueError:
        return "non_numeric_result"
    if not math.isfinite(value):
        return "non_numeric_result"
    if value < 0:
        return "negative_result"
    try:
        low, high = scoring.parse_reference_range(row.get("reference_range") or "")
    except ValueError:
        return "bad_reference_range"
    try:
        collected = parse_timestamp(row.get("collected_at") or "")
    except ValueError:
        return "bad_collected_at"
    direction = scoring.abnormal_direction(value, low, high)
    return LabResult(
        patient, test, value, low, high, direction.upper() if direction else None, collected
    )


def validate_rows(rows: Iterable[Mapping[str, Any]], known: set[str]) -> ValidationResult:
    """Row-level validation, including duplicates.

    An exact repeat of a row is a ``duplicate_row``. Two *different* results for the same
    patient and test in one file keep the most recent ``collected_at`` (a re-sent correction);
    the other is rejected as ``superseded_result``.
    """
    result = ValidationResult()
    seen: set[tuple] = set()
    best: dict[tuple[str, str], tuple[LabResult, dict]] = {}
    for raw in rows:
        result.rows_in += 1
        raw = dict(raw)
        fingerprint = tuple((k, (raw.get(k) or "").strip()) for k in LAB_COLUMNS)
        if fingerprint in seen:
            result.rejected.append((raw, "duplicate_row"))
            continue
        seen.add(fingerprint)
        checked = validate_row(raw, known)
        if isinstance(checked, str):
            result.rejected.append((raw, checked))
            continue
        key = (checked.patient_id, checked.test_type)
        if key in best:
            kept, kept_raw = best[key]
            if checked.collected_at >= kept.collected_at:
                result.rejected.append((kept_raw, "superseded_result"))
                best[key] = (checked, raw)
            else:
                result.rejected.append((raw, "superseded_result"))
            continue
        best[key] = (checked, raw)
    result.rows = sorted((r for r, _ in best.values()), key=lambda r: (r.patient_id, r.test_type))
    return result


def validate_text(text: str, known: set[str]) -> ValidationResult:
    """Validate the content of a lab file (header first)."""
    reader = csv.DictReader(io.StringIO(text))
    header = [h.strip() for h in (reader.fieldnames or [])]
    missing = [c for c in LAB_COLUMNS if c not in header]
    if missing:
        return ValidationResult(file_error=f"missing_columns:{','.join(missing)}")
    reader.fieldnames = header
    return validate_rows(reader, known)


def validate_file(path: Path, known: set[str]) -> ValidationResult:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return ValidationResult(file_error=f"unreadable:{type(exc).__name__}")
    if not text.strip():
        return ValidationResult(file_error="empty_file")
    return validate_text(text, known)


def write_rejects(quarantine: Path, day: int, rejected: list[tuple[dict, str]]) -> Path | None:
    """``quarantine/labs_day_NNN.rejected.csv`` with a ``reject_reason`` column (or nothing)."""
    target = quarantine / lab_filename(day).replace(".csv", ".rejected.csv")
    if not rejected:
        target.unlink(missing_ok=True)  # a replay of a now-clean file leaves no stale rejects
        return None
    quarantine.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=[*LAB_COLUMNS, "reject_reason"], extrasaction="ignore"
        )
        writer.writeheader()
        for row, reason in rejected:
            writer.writerow({**row, "reject_reason": reason})
    return target


def quarantine_file(path: Path, quarantine: Path, reason: str) -> Path:
    target = move(path, quarantine)
    target.with_name(target.name + ".reason.txt").write_text(reason + "\n", encoding="utf-8")
    return target


# ---------------------------------------------------------------------------------- lab risk
def lab_risk_by_patient(rows: Iterable[LabResult]) -> dict[str, tuple[int, list[str]]]:
    """patient -> (lab_risk_points, ["lactate:high", ...]) with the shared scoring rules.

    Every patient with at least one valid result gets a row - also with 0 points, so labs that
    return to normal *lower* the risk in the speed layer again.
    """
    abnormal: dict[str, list[tuple[str, str]]] = {}
    for r in rows:
        abnormal.setdefault(r.patient_id, [])
        if r.direction:
            abnormal[r.patient_id].append((r.test_type, r.direction))
    return {
        pid: (scoring.lab_risk_points(tests), sorted({f"{t}:{d}" for t, d in tests}))
        for pid, tests in abnormal.items()
    }


# ------------------------------------------------------------------------- database writes
def load_lab_results(cur: Any, day: int, rows: list[LabResult], source_file: str) -> int:
    """Replace lab day ``day`` in ``lab_results`` (idempotent)."""
    from psycopg2.extras import execute_values

    cur.execute("DELETE FROM lab_results WHERE sim_day = %s", (day,))
    values = [
        (
            day,
            r.patient_id,
            r.test_type,
            r.result_value,
            r.ref_low,
            r.ref_high,
            r.abnormal_flag,
            r.collected_at,
            source_file,
        )
        for r in rows
    ]
    if values:
        execute_values(
            cur,
            "INSERT INTO lab_results (sim_day, patient_id, test_type, result_value, ref_low, "
            "ref_high, abnormal_flag, collected_at, source_file) VALUES %s",
            values,
        )
    return len(values)


def fetch_lab_results(cur: Any, day: int) -> list[LabResult]:
    cur.execute(
        "SELECT patient_id, test_type, result_value, ref_low, ref_high, abnormal_flag, "
        "collected_at FROM lab_results WHERE sim_day = %s",
        (day,),
    )
    return [LabResult(*r) for r in cur.fetchall()]


def write_lab_risk(cur: Any, day: int, risk: dict[str, tuple[int, list[str]]]) -> int:
    """Replace lab day ``day`` in ``patient_lab_risk`` (the speed layer reads the newest)."""
    from psycopg2.extras import execute_values

    cur.execute("DELETE FROM patient_lab_risk WHERE as_of_sim_day = %s", (day,))
    values = [(pid, points, tests, day) for pid, (points, tests) in sorted(risk.items())]
    if values:
        execute_values(
            cur,
            "INSERT INTO patient_lab_risk (patient_id, lab_risk_points, abnormal_tests, "
            "as_of_sim_day) VALUES %s",
            values,
        )
    return len(values)
