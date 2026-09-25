"""Data contracts (PROJECT_PLAN.md section 4) as code.

These models are the single reference for what travels on ``vitals.raw``, what a
lab CSV row looks like and what lands on ``vitals.dlq``. Numeric fields are
``Optional`` and *not* range-checked here on purpose: the simulator deliberately
emits nulls and out-of-range garbage, and rejecting them is the job of the
speed layer's validation step (which routes them to the DLQ).
"""

from __future__ import annotations

from pydantic import BaseModel

VITAL_FIELDS = ("heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature")

LAB_COLUMNS = ("patient_id", "test_type", "result_value", "reference_range", "collected_at")

# DDL string for Spark's ``from_json`` / ``StructType.fromDDL`` (helper for Member B).
VITALS_SPARK_DDL = (
    "event_id STRING, patient_id STRING, heart_rate INT, spo2 INT, systolic_bp INT, "
    "diastolic_bp INT, temperature DOUBLE, timestamp STRING, sim_day INT"
)


class VitalReading(BaseModel):
    """One bedside-monitor reading; key = ``patient_id``; topic ``vitals.raw``."""

    event_id: str
    patient_id: str
    heart_rate: int | None = None
    spo2: int | None = None
    systolic_bp: int | None = None
    diastolic_bp: int | None = None
    temperature: float | None = None
    timestamp: str  # event time, UTC ISO-8601 with milliseconds and trailing Z
    sim_day: int


class LabRow(BaseModel):
    """One row of the daily lab file ``labs_day_NNN.csv``."""

    patient_id: str
    test_type: str
    result_value: float
    reference_range: str  # "low-high", e.g. "3.5-5.0"
    collected_at: str  # UTC ISO-8601


class DlqEvent(BaseModel):
    """Envelope for records rejected by the speed layer (topic ``vitals.dlq``)."""

    reason: str
    raw_payload: str
    failed_at: str
