"""Response models (they also document the API in the generated OpenAPI spec at ``/docs``)."""

from __future__ import annotations

from datetime import datetime
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel, Field

Tier = Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]
Severity = Literal["MEDIUM", "HIGH", "CRITICAL"]
T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: list[T]
    total: int = Field(description="rows matching the filters (all pages)")
    limit: int
    offset: int


# ------------------------------------------------------------------------------- patients
class PatientSummary(BaseModel):
    patient_id: str
    name: str
    age: int
    sex: str
    ward: str
    bed: str
    comorbidity: str
    risk_tier: Tier | None = Field(None, description="speed-layer tier (NEWS + lab points)")
    news_score: int | None = None
    lab_risk_points: int | None = Field(None, description="points from the latest lab file")
    lab_as_of_sim_day: int | None = None
    total_score: int | None = None
    trend_flag: str | None = Field(None, description="STABLE or e.g. HR_RISING,SPO2_FALLING")
    sustained_abnormal_windows: int | None = None
    heart_rate: int | None = None
    spo2: int | None = None
    systolic_bp: int | None = None
    diastolic_bp: int | None = None
    temperature: float | None = None
    map: float | None = None
    last_reading_at: datetime | None = None
    sim_day: int | None = None
    abnormal_tests: list[str] = Field(default_factory=list)
    open_alerts: int = 0


class Alert(BaseModel):
    alert_id: str
    patient_id: str
    severity: Severity
    reason_code: str
    value: float | None = None
    threshold: float | None = None
    opened_at: datetime
    last_seen_at: datetime | None = None
    resolved_at: datetime | None = None
    status: Literal["open", "resolved"]


class LabResult(BaseModel):
    sim_day: int
    test_type: str
    result_value: float
    ref_low: float
    ref_high: float
    abnormal_flag: Literal["HIGH", "LOW"] | None = None
    collected_at: datetime


class ReportEntry(BaseModel):
    sim_day: int
    patient_id: str
    rank: int
    vitals_sim_day: int
    vitals_source: str
    vitals_summary: str
    lab_summary: str
    news_score: int
    max_vital_score: int
    trend: str | None = None
    lab_points_before: int
    lab_points_after: int
    total_before: int
    total_after: int
    risk_before_labs: Tier
    risk_after_labs: Tier
    tier_change: Literal["UP", "DOWN", "SAME"]
    abnormal_tests: list[str] = Field(default_factory=list)
    alerts_opened: int = 0
    generated_at: datetime | None = None


class PatientDetail(PatientSummary):
    admitted_at: datetime | None = None
    baseline: dict[str, float] = Field(default_factory=dict)
    open_alert_list: list[Alert] = Field(default_factory=list)
    latest_labs: list[LabResult] = Field(default_factory=list)
    latest_report: ReportEntry | None = None


class VitalsWindow(BaseModel):
    window_start: datetime
    window_end: datetime
    n_readings: int
    avg_heart_rate: float | None = None
    min_heart_rate: int | None = None
    max_heart_rate: int | None = None
    avg_spo2: float | None = None
    min_spo2: int | None = None
    max_spo2: int | None = None
    avg_systolic_bp: float | None = None
    min_systolic_bp: int | None = None
    max_systolic_bp: int | None = None
    avg_diastolic_bp: float | None = None
    avg_temperature: float | None = None
    max_temperature: float | None = None
    news_score: int | None = None
    trend_slope_hr: float | None = None
    trend_slope_spo2: float | None = None
    trend_slope_sbp: float | None = None


class PatientVitals(BaseModel):
    patient_id: str
    minutes: int
    windows: list[VitalsWindow]


# ----------------------------------------------------------------------------------- ward
class AvgVitals(BaseModel):
    heart_rate: float | None = None
    spo2: float | None = None
    systolic_bp: float | None = None
    diastolic_bp: float | None = None
    temperature: float | None = None
    news_score: float | None = None


class Freshness(BaseModel):
    last_reading_at: datetime | None = None
    data_age_seconds: float | None = None
    stale: bool
    max_age_seconds: float
    current_sim_day: int | None = None


class BatchStatus(BaseModel):
    latest_report_sim_day: int | None = None
    latest_lab_sim_day: int | None = None
    last_dag_success_at: datetime | None = None
    last_discrepancy_ratio: float | None = None


class ConcerningPatient(BaseModel):
    patient_id: str
    bed: str
    risk_tier: Tier | None
    total_score: int | None
    news_score: int | None
    lab_risk_points: int | None
    trend_flag: str | None
    open_alerts: int


class WardSummary(BaseModel):
    generated_at: datetime
    total_patients: int
    patients_with_data: int
    patients_by_tier: dict[str, int]
    active_alerts: int
    active_alerts_by_severity: dict[str, int]
    avg_vitals: AvgVitals
    readings_per_minute: float | None = Field(
        None, description="from each patient's latest complete window (speed layer)"
    )
    freshness: Freshness
    batch: BatchStatus
    concerning_patients: list[ConcerningPatient] = Field(
        description="tier HIGH/CRITICAL or a worsening trend, most at risk first"
    )


# -------------------------------------------------------------------------------- reports
class ReportDay(BaseModel):
    sim_day: int
    patients: int
    tier_changes: int
    generated_at: datetime


class RiskReport(BaseModel):
    sim_day: int
    vitals_sim_day: int
    generated_at: datetime
    discrepancy_ratio: float | None = Field(
        None, description="speed-vs-batch discrepancy of the vitals day (Lambda reconciliation)"
    )
    tier_counts: dict[str, int]
    tier_changes: int
    html_url: str | None = None
    patients: list[ReportEntry]


class PipelineRun(BaseModel):
    id: int
    stage: str
    run_id: str
    sim_day: int | None = None
    status: str
    rows_in: int | None = None
    rows_out: int | None = None
    details: dict = Field(default_factory=dict)
    started_at: datetime
    finished_at: datetime | None = None


class Health(BaseModel):
    status: Literal["ok", "degraded", "down"]
    database: Literal["ok", "unavailable"]
    data_age_seconds: float | None = None
    max_age_seconds: float
    detail: str | None = None
