"""Patient alert engine (task B7): rules, severity, reason codes, cooldown, lifecycle.

Rules (contract 4.5):

=====================  ==============================================  ==========
reason_code            condition                                       severity
=====================  ==============================================  ==========
HR_CRITICAL, ...       a single vital scores 3 (HR/SPO2/SBP/TEMP)      HIGH
TOTAL_SCORE_HIGH       news_score + lab_risk_points >= 5               HIGH, CRITICAL if >= 7
SUSTAINED_ABNORMAL     window news >= 3 in >= 3 consecutive windows    HIGH
WORSENING_TREND        trend flag set (HR up / SpO2 down / SBP down /  MEDIUM
                       window news rising)
=====================  ==============================================  ==========

Lifecycle: at most one *open* alert per (patient, reason). While the condition holds the
open alert is *touched* (``last_seen_at``, worst value/severity); once the condition is
false at the patient's newest data point it is *resolved*. A new alert for the same
(patient, reason) is suppressed for 60 s after the previous one resolved (cooldown), so a
value hovering around a threshold does not flood the ward with alerts.

The engine is pure: it gets the open alerts / last resolution times from the database and
returns actions; ``sinks.apply_alert_actions`` executes them. All times are **event times**
and alert ids are ``uuid5(patient|reason|opened_at)``, so replaying a micro-batch after a
crash re-derives the same actions and the inserts are no-ops.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from common import scoring

SEVERITY_ORDER = {"MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
ALERT_NAMESPACE = uuid.UUID("5b0c6c1e-8f59-4d8e-9a55-7d2d8f7c1a01")

# vital -> reason code for the single-vital rule
VITAL_REASONS = {
    "heart_rate": "HR_CRITICAL",
    "spo2": "SPO2_CRITICAL",
    "systolic_bp": "SBP_CRITICAL",
    "temperature": "TEMP_CRITICAL",
}
READING_REASONS = (*VITAL_REASONS.values(), "TOTAL_SCORE_HIGH")
WINDOW_REASONS = ("SUSTAINED_ABNORMAL", "WORSENING_TREND")


@dataclass(frozen=True)
class Signal:
    """What one micro-batch says about one rule for one patient.

    triggered_at  first event time in the batch at which the condition held (None = never)
    active        condition holds at the patient's newest data point; None = unknown (e.g.
                  the batch only had late readings older than what is already stored)
    """

    reason: str
    severity: str
    value: float | None
    threshold: float
    triggered_at: datetime | None
    active: bool | None


@dataclass(frozen=True)
class OpenAlert:
    alert_id: str
    severity: str
    value: float | None
    opened_at: datetime


@dataclass
class Actions:
    opens: list[dict] = field(default_factory=list)
    touches: list[dict] = field(default_factory=list)
    resolves: list[dict] = field(default_factory=list)
    suppressed: int = 0


def alert_id(patient_id: str, reason: str, opened_at: datetime) -> str:
    return str(uuid.uuid5(ALERT_NAMESPACE, f"{patient_id}|{reason}|{opened_at.isoformat()}"))


def worse_severity(a: str, b: str) -> str:
    return a if SEVERITY_ORDER[a] >= SEVERITY_ORDER[b] else b


def evaluate(
    patient_id: str,
    signals: list[Signal],
    latest_at: datetime,
    open_alerts: dict[str, OpenAlert],
    last_resolved: dict[str, datetime],
    cooldown: timedelta = timedelta(seconds=scoring.ALERT_COOLDOWN_SECONDS),
    actions: Actions | None = None,
) -> Actions:
    """Decide open / touch / resolve for one patient; state dicts are keyed by reason."""
    actions = actions or Actions()
    for sig in signals:
        current = open_alerts.get(sig.reason)
        if current is not None:
            if sig.triggered_at is not None or sig.active:
                actions.touches.append(
                    {
                        "alert_id": current.alert_id,
                        "last_seen_at": latest_at,
                        "severity": worse_severity(current.severity, sig.severity),
                        "value": sig.value if sig.value is not None else current.value,
                    }
                )
            if sig.active is False:
                actions.resolves.append({"alert_id": current.alert_id, "resolved_at": latest_at})
            continue

        if sig.triggered_at is None:
            continue
        resolved = last_resolved.get(sig.reason)
        if resolved is not None and sig.triggered_at < resolved + cooldown:
            actions.suppressed += 1
            continue
        new_id = alert_id(patient_id, sig.reason, sig.triggered_at)
        actions.opens.append(
            {
                "alert_id": new_id,
                "patient_id": patient_id,
                "severity": sig.severity,
                "reason_code": sig.reason,
                "value": sig.value,
                "threshold": sig.threshold,
                "opened_at": sig.triggered_at,
                "last_seen_at": max(latest_at, sig.triggered_at),
            }
        )
        if sig.active is False:  # a transient spike: opened and already over in this batch
            actions.resolves.append(
                {"alert_id": new_id, "resolved_at": max(latest_at, sig.triggered_at)}
            )
    return actions


# ---------------------------------------------------------------- signal builders
def reading_signals(summary: dict) -> list[Signal]:
    """Signals from the per-patient micro-batch summary built by the readings query.

    ``summary`` keys: ``<vital>_first3_at`` / ``<vital>_worst`` (first time the vital scored 3
    and its most extreme value), ``<vital>_score_latest``, ``total_first_high_at``,
    ``total_max``, ``total_latest``, ``is_newest`` (batch holds the newest reading).
    """
    newest = summary.get("is_newest", True)
    signals = []
    for vital, reason in VITAL_REASONS.items():
        latest_score = summary.get(f"{vital}_score_latest")
        signals.append(
            Signal(
                reason=reason,
                severity="HIGH",
                value=summary.get(f"{vital}_worst"),
                threshold=scoring.CRITICAL_VITAL_SCORE,
                triggered_at=summary.get(f"{vital}_first3_at"),
                active=(latest_score >= scoring.CRITICAL_VITAL_SCORE) if newest else None,
            )
        )
    total_max = summary.get("total_max") or 0
    signals.append(
        Signal(
            reason="TOTAL_SCORE_HIGH",
            severity="CRITICAL" if total_max >= scoring.TIERS[0][0] else "HIGH",
            value=total_max,
            threshold=scoring.ALERT_TOTAL_SCORE,
            triggered_at=summary.get("total_first_high_at"),
            active=(summary.get("total_latest", 0) >= scoring.ALERT_TOTAL_SCORE)
            if newest
            else None,
        )
    )
    return signals


def window_signals(sustained_windows: int, flags: list[str], at: datetime) -> list[Signal]:
    """Signals from the trend analysis of the newest window (``at`` = its window end)."""
    sustained = sustained_windows >= scoring.SUSTAINED_WINDOWS
    worsening = bool(flags)
    return [
        Signal(
            reason="SUSTAINED_ABNORMAL",
            severity="HIGH",
            value=sustained_windows,
            threshold=scoring.SUSTAINED_WINDOWS,
            triggered_at=at if sustained else None,
            active=sustained,
        ),
        Signal(
            reason="WORSENING_TREND",
            severity="MEDIUM",
            value=len(flags),
            threshold=1,
            triggered_at=at if worsening else None,
            active=worsening,
        ),
    ]
