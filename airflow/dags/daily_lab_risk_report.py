"""DAG ``daily_lab_risk_report`` - the batch layer's daily chain (task C3, owner: Member C).

    resolve_sim_day -> wait_for_lab_file -> validate_lab_file -> load_lab_results
      -> compute_lab_risk -> batch_vitals_job -> build_risk_report
      -> data_quality_and_health_check -> archive_file

One run per simulated day (schedule = ``SIM_DAY_SECONDS``, default 5 real minutes). A run handles
lab file day N (the current sim day, or ``{"sim_day": N}`` in the trigger conf for a replay):

    airflow dags trigger daily_lab_risk_report -c '{"sim_day": 3}'

* ``wait_for_lab_file`` is a ``FileSensor`` (reschedule mode) on ``**/labs_day_NNN.csv`` below the
  landing zone, so a replay also finds the file in ``processed/``. Timeout = 80 % of a sim day;
  a timeout calls ``on_task_failure`` -> ``lab_file_missing_total`` -> alert ``LabFileMissing``.
* every task retries twice (10 s apart), has an SLA of one sim day and an execution timeout;
  failures log a JSON event and push ``airflow_task_failures_total``.
* the last task pushes ``airflow_dag_last_success_timestamp`` (alert ``DagFailed``).

All logic lives in ``batch/tasks.py``; this file only wires it (fast to parse, easy to test).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

from batch import tasks
from batch.settings import DAG_ID

SIM_DAY_SECONDS = float(os.environ.get("SIM_DAY_SECONDS") or 300)
SENSOR_TIMEOUT = float(os.environ.get("LAB_SENSOR_TIMEOUT_SECONDS") or 0.8 * SIM_DAY_SECONDS)
SENSOR_POKE = float(os.environ.get("LAB_SENSOR_POKE_SECONDS") or 10)

default_args = {
    "owner": "member-c",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "execution_timeout": timedelta(seconds=max(SIM_DAY_SECONDS, 240)),
    "sla": timedelta(seconds=SIM_DAY_SECONDS),
    "on_failure_callback": tasks.on_task_failure,
}

with DAG(
    dag_id=DAG_ID,
    description="Daily lab file -> lab risk -> batch recompute -> consolidated risk report",
    schedule=timedelta(seconds=SIM_DAY_SECONDS),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=tasks.on_sla_miss,
    dagrun_timeout=timedelta(seconds=3 * SIM_DAY_SECONDS),
    tags=["batch-layer", "member-c"],
    doc_md=__doc__,
) as dag:
    resolve = PythonOperator(task_id="resolve_sim_day", python_callable=tasks.resolve_sim_day)

    wait = FileSensor(
        task_id="wait_for_lab_file",
        fs_conn_id="fs_landing",
        filepath=(
            "**/labs_day_{{ '%03d' | format(ti.xcom_pull(task_ids='resolve_sim_day') | int) }}.csv"
        ),
        recursive=True,
        mode="reschedule",
        poke_interval=SENSOR_POKE,
        timeout=SENSOR_TIMEOUT,
        retries=0,  # a timeout is the signal (missing file), not a transient error
        soft_fail=False,
    )

    steps = [
        PythonOperator(task_id=name, python_callable=getattr(tasks, name))
        for name in (
            "validate_lab_file",
            "load_lab_results",
            "compute_lab_risk",
            "batch_vitals_job",
            "build_risk_report",
            "data_quality_and_health_check",
            "archive_file",
        )
    ]

    resolve >> wait >> steps[0]
    for upstream, downstream in zip(steps, steps[1:], strict=False):
        upstream >> downstream
