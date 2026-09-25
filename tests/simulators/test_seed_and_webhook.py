import importlib.util
import json
from pathlib import Path

from simulators.patients import build_patients
from simulators.seed_patients import UPSERT, patient_row

ROOT = Path(__file__).resolve().parents[2]


def _load_webhook():
    spec = importlib.util.spec_from_file_location(
        "alert_webhook_app", ROOT / "observability" / "alert_webhook" / "app.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_patient_rows_fill_every_upsert_parameter_and_hide_the_story():
    patients = build_patients(20, seed=42)
    rows = [patient_row(p) for p in patients]
    assert len({r["patient_id"] for r in rows}) == 20
    for row in rows:
        for column in row:
            assert f"%({column})s" in UPSERT  # every value has a placeholder
        # ground truth (episodes, occult risk) must never reach the database
        assert not {"episodes", "occult_from_day", "is_deteriorating"} & set(row)
        assert isinstance(row["comorbidity_flag"], bool)


def test_patients_ddl_matches_columns_written_by_the_seeder():
    ddl = (ROOT / "sql" / "01_patients.sql").read_text()
    for column in patient_row(build_patients(1, 1, num_deteriorating=0, num_occult=0)[0]):
        assert column in ddl


def test_webhook_flattens_alertmanager_payload_into_log_records():
    webhook = _load_webhook()
    payload = {
        "status": "firing",
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "NoVitalsData",
                    "severity": "critical",
                    "stage": "ingestion",
                },
                "annotations": {"summary": "No data", "description": "45s"},
                "startsAt": "2026-01-01T00:00:00Z",
                "endsAt": "0001-01-01T00:00:00Z",
            },
            {
                "status": "resolved",
                "labels": {"alertname": "ApiDown", "severity": "critical"},
                "annotations": {},
            },
        ],
    }
    firing, resolved = webhook.flatten(payload)
    assert firing["event"] == "alert_firing" and firing["level"] == "error"
    assert firing["alertname"] == "NoVitalsData" and firing["stage"] == "observability"
    assert firing["summary"] == "No data"
    assert resolved["event"] == "alert_resolved" and resolved["level"] == "info"
    json.dumps(firing)  # must be serialisable as one log line


def test_webhook_emit_appends_jsonl(tmp_path, monkeypatch, capsys):
    webhook = _load_webhook()
    monkeypatch.setattr(webhook, "ALERT_LOG", str(tmp_path / "alerts" / "alerts.jsonl"))
    webhook.emit({"event": "alert_firing", "alertname": "X"})
    webhook.emit({"event": "alert_resolved", "alertname": "X"})
    lines = (tmp_path / "alerts" / "alerts.jsonl").read_text().splitlines()
    assert [json.loads(line)["event"] for line in lines] == ["alert_firing", "alert_resolved"]
    assert capsys.readouterr().out.count("alert_") == 2  # also echoed to stdout for docker logs


def test_webhook_survives_unwritable_log(monkeypatch, capsys):
    webhook = _load_webhook()
    monkeypatch.setattr(webhook, "ALERT_LOG", "/proc/definitely/not/writable/alerts.jsonl")
    webhook.emit({"event": "alert_firing"})  # must not raise
    assert "alert_firing" in capsys.readouterr().out
