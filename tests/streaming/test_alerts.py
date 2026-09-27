"""Alert engine lifecycle: open, touch, resolve, cooldown, spikes, replay safety."""

from datetime import UTC, datetime, timedelta

from streaming import alerts
from streaming.alerts import OpenAlert, Signal

T0 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC)


def sig(triggered=None, active=None, reason="HR_CRITICAL", severity="HIGH", value=135):
    return Signal(reason, severity, value, 3, triggered, active)


def test_opens_once_and_touches_while_condition_holds():
    first = alerts.evaluate("P001", [sig(T0, True)], T0, {}, {})
    assert len(first.opens) == 1 and not first.resolves
    opened = first.opens[0]
    assert opened["opened_at"] == T0 and opened["reason_code"] == "HR_CRITICAL"

    current = {"HR_CRITICAL": OpenAlert(opened["alert_id"], "HIGH", 135, T0)}
    later = T0 + timedelta(seconds=10)
    second = alerts.evaluate("P001", [sig(later, True, value=140)], later, current, {})
    assert not second.opens
    assert second.touches[0]["value"] == 140


def test_resolves_when_newest_reading_is_normal():
    current = {"HR_CRITICAL": OpenAlert("a1", "HIGH", 135, T0)}
    later = T0 + timedelta(seconds=20)
    out = alerts.evaluate("P001", [sig(None, False)], later, current, {})
    assert out.resolves == [{"alert_id": "a1", "resolved_at": later}]


def test_unknown_state_neither_opens_duplicate_nor_resolves():
    current = {"HR_CRITICAL": OpenAlert("a1", "HIGH", 135, T0)}
    out = alerts.evaluate("P001", [sig(None, None)], T0, current, {})
    assert not out.opens and not out.resolves and not out.touches


def test_transient_spike_opens_and_resolves_in_one_batch():
    spike_at = T0
    latest = T0 + timedelta(seconds=4)
    out = alerts.evaluate("P001", [sig(spike_at, False)], latest, {}, {})
    assert len(out.opens) == 1
    assert out.resolves[0]["alert_id"] == out.opens[0]["alert_id"]


def test_cooldown_suppresses_reopening_within_60s():
    resolved = {"HR_CRITICAL": T0}
    within = alerts.evaluate("P001", [sig(T0 + timedelta(seconds=59), True)], T0, {}, resolved)
    assert not within.opens and within.suppressed == 1
    after = alerts.evaluate("P001", [sig(T0 + timedelta(seconds=60), True)], T0, {}, resolved)
    assert len(after.opens) == 1


def test_alert_id_is_deterministic_for_replays():
    a = alerts.evaluate("P001", [sig(T0, True)], T0, {}, {}).opens[0]["alert_id"]
    b = alerts.evaluate("P001", [sig(T0, True)], T0, {}, {}).opens[0]["alert_id"]
    assert a == b
    c = alerts.evaluate("P002", [sig(T0, True)], T0, {}, {}).opens[0]["alert_id"]
    assert a != c


def test_severity_escalates_on_touch():
    current = {"TOTAL_SCORE_HIGH": OpenAlert("a1", "HIGH", 5, T0)}
    s = sig(T0, True, reason="TOTAL_SCORE_HIGH", severity="CRITICAL", value=8)
    out = alerts.evaluate("P001", [s], T0, current, {})
    assert out.touches[0]["severity"] == "CRITICAL"


def test_reading_signals_from_summary():
    summary = {
        "heart_rate_first3_at": T0,
        "heart_rate_worst": 140,
        "heart_rate_score_latest": 3,
        "spo2_score_latest": 0,
        "systolic_bp_score_latest": 0,
        "temperature_score_latest": 0,
        "total_first_high_at": T0,
        "total_max": 7,
        "total_latest": 7,
        "is_newest": True,
    }
    by_reason = {s.reason: s for s in alerts.reading_signals(summary)}
    assert by_reason["HR_CRITICAL"].active is True
    assert by_reason["SPO2_CRITICAL"].triggered_at is None
    assert by_reason["TOTAL_SCORE_HIGH"].severity == "CRITICAL"
    late = alerts.reading_signals({**summary, "is_newest": False})
    assert all(s.active is None for s in late)


def test_window_signals():
    by_reason = {s.reason: s for s in alerts.window_signals(3, ["HR_RISING"], T0)}
    assert by_reason["SUSTAINED_ABNORMAL"].triggered_at == T0
    assert by_reason["WORSENING_TREND"].severity == "MEDIUM"
    calm = {s.reason: s for s in alerts.window_signals(2, [], T0)}
    assert calm["SUSTAINED_ABNORMAL"].active is False
    assert calm["WORSENING_TREND"].triggered_at is None
