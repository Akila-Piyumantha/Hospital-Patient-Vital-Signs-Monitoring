"""Pushgateway metrics of the batch layer: persistent counters and exposition format."""

from __future__ import annotations

import pytest

pytest.importorskip("prometheus_client")

from prometheus_client import generate_latest  # noqa: E402

from batch import metrics  # noqa: E402


def test_counters_survive_new_processes(tmp_path):
    store = metrics.counter_store(str(tmp_path))
    store.inc("wait_for_lab_file", "lab_file_missing_total")
    store.inc("load_lab_results", "lab_files_ingested_total", 2)
    again = metrics.counter_store(str(tmp_path))  # "next task process"
    assert again.inc("wait_for_lab_file", "lab_file_missing_total") == {
        "lab_file_missing_total": 2.0
    }
    assert again.read()["load_lab_results"] == {"lab_files_ingested_total": 2.0}


def test_corrupt_state_file_starts_from_zero(tmp_path):
    (tmp_path / "batch_metrics.json").write_text("{not json")
    store = metrics.counter_store(str(tmp_path))
    assert store.inc("t", "lab_file_missing_total") == {"lab_file_missing_total": 1.0}


def test_registry_uses_contract_names_and_dag_label():
    registry = metrics.build_registry(
        {"speed_batch_discrepancy_ratio": 0.042, "airflow_dag_last_success_timestamp": 1.7e9},
        {"lab_file_missing_total": 3.0},
    )
    text = generate_latest(registry).decode()
    assert 'speed_batch_discrepancy_ratio{dag_id="daily_lab_risk_report"} 0.042' in text
    assert 'lab_file_missing_total{dag_id="daily_lab_risk_report"} 3.0' in text
    assert "airflow_dag_last_success_timestamp{" in text


def test_push_failure_is_not_fatal():
    assert metrics.push("127.0.0.1:9", "t", gauges={"batch_vitals_readings": 1.0}) is False
