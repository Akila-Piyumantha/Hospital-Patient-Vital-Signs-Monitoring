import io
import json
import logging

from common.config import Settings, env_bool, env_int_set
from common.logging_setup import configure_logging, get_logger


def test_json_log_line_has_contract_fields():
    stream = io.StringIO()
    run_id = configure_logging("unit-test", "INFO", run_id="abc123", stream=stream)
    get_logger("t", "ingestion", patient_id="P001").info("hello", rows=3)
    record = json.loads(stream.getvalue().strip())
    assert run_id == "abc123"
    assert record["service"] == "unit-test"
    assert record["stage"] == "ingestion"
    assert record["event"] == "hello"
    assert record["level"] == "info"
    assert record["rows"] == 3 and record["patient_id"] == "P001"
    assert record["ts"].endswith("Z") and record["run_id"] == "abc123"
    logging.getLogger().handlers.clear()


def test_third_party_loggers_get_the_default_stage():
    stream = io.StringIO()
    configure_logging("unit-test", "INFO", stream=stream, default_stage="ingestion")
    logging.getLogger("librdkafka").warning("GETPID retrying")
    record = json.loads(stream.getvalue().strip())
    assert record["stage"] == "ingestion" and record["event"] == "GETPID retrying"
    logging.getLogger().handlers.clear()


def test_reserved_keys_cannot_be_overwritten_and_errors_carry_traceback():
    stream = io.StringIO()
    configure_logging("unit-test", "INFO", stream=stream)
    log = get_logger("t", "storage")
    try:
        raise ValueError("boom")
    except ValueError:
        log.error("failed", exc_info=True, level="fake")
    record = json.loads(stream.getvalue().strip())
    assert record["level"] == "error" and record["field_level"] == "fake"
    assert "ValueError: boom" in record["error"]
    logging.getLogger().handlers.clear()


def test_env_helpers_treat_empty_as_unset(monkeypatch):
    monkeypatch.setenv("X_EMPTY", "")
    monkeypatch.setenv("X_SET", "on")
    monkeypatch.setenv("X_DAYS", "3, 5,7")
    assert env_bool("X_EMPTY", True) is True
    assert env_bool("X_SET", False) is True
    assert env_int_set("X_DAYS") == frozenset({3, 5, 7})
    assert env_int_set("X_MISSING") == frozenset()


def test_settings_defaults_and_overrides(monkeypatch):
    monkeypatch.setenv("SIM_DAY_SECONDS", "120")
    monkeypatch.setenv("NUM_PATIENTS", "8")
    monkeypatch.delenv("VITALS_TOPIC", raising=False)
    s = Settings.from_env()
    assert s.sim_day_seconds == 120.0 and s.num_patients == 8
    assert s.vitals_topic == "vitals.raw" and s.dlq_topic == "vitals.dlq"
