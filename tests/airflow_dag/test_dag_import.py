"""DAG import test (task C8): the DAG parses, has the planned chain, retries, sensor, SLA.

Needs Airflow (skipped otherwise); CI job ``dag-import`` installs it with the official constraints.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

# "airflow.models", not "airflow": the repo's airflow/ folder is importable as a namespace package
pytest.importorskip("airflow.models")

from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"
CHAIN = [
    "resolve_sim_day",
    "wait_for_lab_file",
    "validate_lab_file",
    "load_lab_results",
    "compute_lab_risk",
    "batch_vitals_job",
    "build_risk_report",
    "data_quality_and_health_check",
    "archive_file",
]


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("daily_lab_risk_report")


def test_chain(dag):
    assert dag is not None
    assert sorted(dag.task_ids) == sorted(CHAIN)
    for upstream, downstream in zip(CHAIN, CHAIN[1:], strict=False):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


def test_schedule_is_one_sim_day_and_no_catchup(dag):
    assert dag.schedule_interval == timedelta(seconds=300)
    assert dag.catchup is False and dag.max_active_runs == 1


def test_retries_sla_and_failure_callback(dag):
    for task in dag.tasks:
        assert task.on_failure_callback is not None, task.task_id
        assert task.sla == timedelta(seconds=300), task.task_id
        if task.task_id != "wait_for_lab_file":
            assert task.retries == 2, task.task_id


def test_sensor(dag):
    sensor = dag.get_task("wait_for_lab_file")
    assert type(sensor).__name__ == "FileSensor"
    assert sensor.mode == "reschedule" and sensor.retries == 0 and sensor.recursive
    assert sensor.timeout == pytest.approx(240)
    assert "labs_day_" in sensor.filepath
