"""Callables of the Airflow DAG ``daily_lab_risk_report`` (task C3).

The DAG file (``airflow/dags/daily_lab_risk_report.py``) only wires these together, so the logic
is importable and testable without Airflow. Every callable

* takes the Airflow context as keyword arguments (``ti``, ``run_id``, ``dag_run`` ...),
* writes one ``pipeline_run_log`` row, logs structured JSON (contract 4.8) and
* is idempotent: re-running a task, or the whole DAG for the same day, gives the same result.

Day numbering: the run processes lab file day ``N`` (``labs_day_N.csv``, labs collected on day
N-1) and recomputes the vitals of day ``N-1``. ``N`` is the current simulated day, or
``dag_run.conf["sim_day"]`` for a replay/backfill.
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from batch import db, lab_pipeline, metrics
from batch.settings import DAG_ID, BatchSettings, read_clock
from common.logging_setup import JsonFormatter, get_logger

log = get_logger("batch.tasks", "orchestration")
store_log = get_logger("batch.tasks", "storage")
proc_log = get_logger("batch.tasks", "processing")

_HANDLER_ATTR = "_hospital_json_handler"


def setup_logging(run_id: str, level: str = "INFO") -> None:
    """JSON lines on stdout for the ``batch`` loggers (Airflow copies stdout into the task log).

    The root logger is left alone - Airflow's own task-log handlers live there.
    """
    logger = logging.getLogger("batch")
    old = getattr(logger, _HANDLER_ATTR, None)
    if old is not None:
        logger.removeHandler(old)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter("airflow-batch", run_id, default_stage="orchestration"))
    logger.addHandler(handler)
    setattr(logger, _HANDLER_ATTR, handler)
    logger.setLevel(level)
    logger.propagate = False


def _ctx(context: dict) -> tuple[BatchSettings, str]:
    settings = BatchSettings.from_env()
    run_id = str(context.get("run_id") or "manual")
    setup_logging(run_id, settings.base.log_level)
    return settings, run_id


def _day(context: dict) -> int:
    ti = context["ti"]
    return int(ti.xcom_pull(task_ids="resolve_sim_day"))


def _validation(context: dict) -> dict:
    return context["ti"].xcom_pull(task_ids="validate_lab_file") or {}


# ----------------------------------------------------------------------------------- tasks
def resolve_sim_day(**context: Any) -> int:
    """Pick the lab day to process and make sure the batch schema exists.

    ``{"sim_day": N}`` in the trigger conf wins (replay/backfill). A scheduled run maps its
    ``data_interval_end`` onto the sim clock: those are exactly one sim day apart, so consecutive
    runs get consecutive days even when the scheduler starts a run a little late. A manual run
    without conf takes the current sim day.
    """
    settings, run_id = _ctx(context)
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    interval_end = context.get("data_interval_end")
    if conf.get("sim_day") is not None:
        day, source = int(conf["sim_day"]), "dag_run.conf"
    elif interval_end is not None and getattr(dag_run, "run_type", None) == "scheduled":
        # the first run after start-up may cover an interval that ended before the sim epoch
        day = max(1, read_clock(settings.base).sim_day(interval_end.timestamp()))
        source = "data_interval_end"
    else:
        day, source = read_clock(settings.base).sim_day(), "sim_clock"
    db.ensure_schema(settings.pg)
    metrics.ensure_counters(
        settings.pushgateway_url,
        settings.base.state_dir,
        {
            "wait_for_lab_file": ("lab_file_missing_total", "airflow_task_failures_total"),
            "validate_lab_file": ("lab_files_quarantined_total", "lab_rows_quarantined_total"),
            "load_lab_results": ("lab_files_ingested_total",),
        },
    )
    with db.RunLog(settings.pg, "resolve_sim_day", run_id, day) as run:
        run.rows(source=source)
    log.info(
        "sim_day_resolved", sim_day=day, source=source, lab_file=lab_pipeline.lab_filename(day)
    )
    return day


def validate_lab_file(**context: Any) -> dict:
    """Schema, types, duplicates and unknown patients; bad rows/files go to ``quarantine/``."""
    settings, run_id = _ctx(context)
    day = _day(context)
    with db.RunLog(settings.pg, "validate_lab_file", run_id, day) as run:
        found = lab_pipeline.find_lab_file(settings.landing_dir, day)
        if found is None:  # the sensor saw it, so someone moved it in between
            raise FileNotFoundError(f"{lab_pipeline.lab_filename(day)} vanished after the sensor")
        path, place = found
        result = lab_pipeline.validate_file(path, db.known_patients(settings.pg))
        if result.file_error:
            if place != "quarantine":
                path = lab_pipeline.quarantine_file(
                    path, settings.quarantine_dir, result.file_error
                )
            metrics.inc_and_push(
                settings.pushgateway_url,
                settings.base.state_dir,
                "validate_lab_file",
                "lab_files_quarantined_total",
            )
            run.rows(rows_in=0, rows_out=0, file_error=result.file_error, file=str(path))
            log.error(
                "lab_file_quarantined",
                sim_day=day,
                file=str(path),
                reason=result.file_error,
            )
            return {"status": "quarantined", "file": str(path), "reason": result.file_error}

        if place == "quarantine":  # a file that was fixed by hand is taken back into the flow
            path = lab_pipeline.move(path, settings.landing_dir)
            place = "landing"
        rejects = lab_pipeline.write_rejects(settings.quarantine_dir, day, result.rejected)
        if result.rejected:
            metrics.inc_and_push(
                settings.pushgateway_url,
                settings.base.state_dir,
                "validate_lab_file",
                "lab_rows_quarantined_total",
                len(result.rejected),
            )
        summary = {
            "status": "ok",
            "file": str(path),
            "place": place,
            "rows_in": result.rows_in,
            "rows_valid": len(result.rows),
            "rejected": result.reject_counts,
            "rejects_file": str(rejects) if rejects else None,
        }
        run.rows(rows_in=result.rows_in, rows_out=len(result.rows), rejected=result.reject_counts)
        level = log.warning if result.rejected else log.info
        level("lab_file_validated", sim_day=day, **summary)
        return summary


def load_lab_results(**context: Any) -> int:
    """Replace the day in ``lab_results`` with the validated rows (idempotent)."""
    settings, run_id = _ctx(context)
    day = _day(context)
    validation = _validation(context)
    with db.RunLog(settings.pg, "load_lab_results", run_id, day) as run:
        if validation.get("status") != "ok":
            run.skip(reason="file_quarantined")
            store_log.warning("lab_load_skipped", sim_day=day, reason="file_quarantined")
            return 0
        path = Path(validation["file"])
        result = lab_pipeline.validate_file(path, db.known_patients(settings.pg))
        with db.connect(settings.pg) as conn, conn.cursor() as cur:
            loaded = lab_pipeline.load_lab_results(cur, day, result.rows, path.name)
        metrics.inc_and_push(
            settings.pushgateway_url,
            settings.base.state_dir,
            "load_lab_results",
            "lab_files_ingested_total",
        )
        run.rows(rows_in=result.rows_in, rows_out=loaded)
        abnormal = sum(1 for r in result.rows if r.abnormal_flag)
        store_log.info(
            "lab_results_loaded", sim_day=day, rows=loaded, abnormal_rows=abnormal, file=path.name
        )
        return loaded


def compute_lab_risk(**context: Any) -> dict:
    """``lab_results`` of the day -> ``patient_lab_risk`` (read by the speed layer, B8)."""
    settings, run_id = _ctx(context)
    day = _day(context)
    with db.RunLog(settings.pg, "compute_lab_risk", run_id, day) as run:
        if _validation(context).get("status") != "ok":
            run.skip(reason="file_quarantined")
            proc_log.warning("lab_risk_skipped", sim_day=day, reason="previous lab risk stays")
            return {"patients": 0, "patients_with_points": 0}
        with db.connect(settings.pg) as conn, conn.cursor() as cur:
            rows = lab_pipeline.fetch_lab_results(cur, day)
            risk = lab_pipeline.lab_risk_by_patient(rows)
            written = lab_pipeline.write_lab_risk(cur, day, risk)
        at_risk = {pid: pts for pid, (pts, _) in risk.items() if pts > 0}
        run.rows(rows_in=len(rows), rows_out=written, patients_with_points=len(at_risk))
        proc_log.info(
            "lab_risk_computed",
            sim_day=day,
            patients=written,
            patients_with_points=len(at_risk),
            points=dict(sorted(at_risk.items())),
        )
        return {"patients": written, "patients_with_points": len(at_risk)}


def batch_vitals_job(**context: Any) -> dict:
    """Recompute day N-1 from the lake and reconcile it with the speed layer (C4)."""
    from dataclasses import asdict

    from batch import daily_vitals_job

    settings, run_id = _ctx(context)
    day = _day(context)
    vitals_day = day - 1
    with db.RunLog(settings.pg, "batch_vitals_job", run_id, vitals_day) as run:
        if vitals_day < 1:
            run.skip(reason="no vitals day before day 1")
            proc_log.info("batch_vitals_skipped", sim_day=vitals_day, reason="before day 1")
            return {"status": "skipped", "sim_day": vitals_day}
        waited = _wait_for_day_to_settle(settings, vitals_day)
        result = daily_vitals_job.run(settings, vitals_day)
        run.rows(rows_in=result.lake_rows, rows_out=result.patients, **asdict(result))
        gauges = {"batch_vitals_readings": float(result.readings)}
        if result.reconciliation is not None:
            ratio = result.reconciliation["discrepancy_ratio"]
            gauges["speed_batch_discrepancy_ratio"] = ratio
        metrics.push(settings.pushgateway_url, "batch_vitals_job", gauges=gauges)
        proc_log.info("batch_day_recomputed", waited_s=round(waited, 1), **asdict(result))
        if result.reconciliation and (
            result.reconciliation["discrepancy_ratio"] > settings.discrepancy_alert_ratio
        ):
            proc_log.warning(
                "speed_batch_discrepancy_high", sim_day=vitals_day, **result.reconciliation
            )
        return asdict(result)


def _wait_for_day_to_settle(settings: BatchSettings, vitals_day: int) -> float:
    """Sleep until the speed layer can have finished ``vitals_day`` (day end + settle time)."""
    try:
        clock = read_clock(settings.base)
    except RuntimeError:
        return 0.0  # replay without a running simulator: the day is long settled
    ready_at = clock.day_end(vitals_day) + settings.settle_seconds
    wait = ready_at - time.time()
    if wait > 0:
        proc_log.info("waiting_for_speed_layer", sim_day=vitals_day, wait_s=round(wait, 1))
        time.sleep(wait)
    return max(wait, 0.0)


def build_risk_report(**context: Any) -> dict:
    """``patient_risk_report`` rows + HTML + CSV for the day (C5)."""
    from batch import report

    settings, run_id = _ctx(context)
    day = _day(context)
    with db.RunLog(settings.pg, "build_risk_report", run_id, day) as run:
        bounds = None
        try:
            clock = read_clock(settings.base)
            bounds = (
                datetime.fromtimestamp(clock.day_start(day - 1), tz=UTC),
                datetime.fromtimestamp(clock.day_end(day - 1), tz=UTC),
            )
        except RuntimeError:
            pass
        with db.connect(settings.pg) as conn, conn.cursor() as cur:
            inputs = report.fetch_inputs(cur, day, bounds)
            discrepancy = inputs.pop("discrepancy_ratio")
            rows = report.build_report(day, **inputs)
            written = report.write_report_table(cur, day, rows)
        html_path, csv_path = report.report_paths(settings.reports_dir, day)
        report.write_html(html_path, report.render_html(day, rows, discrepancy, report.now_utc()))
        report.write_csv(csv_path, rows)
        changed = [
            {"patient_id": r.patient_id, "before": r.risk_before_labs, "after": r.risk_after_labs}
            for r in rows
            if r.tier_change != "SAME"
        ]
        run.rows(rows_out=written, html=str(html_path), tier_changes=len(changed))
        store_log.info(
            "risk_report_built",
            sim_day=day,
            rows=written,
            html=str(html_path),
            csv=str(csv_path),
            tier_changes=changed,
            top=[r.patient_id for r in rows[:3]],
        )
        return {"rows": written, "html": str(html_path), "tier_changes": len(changed)}


def data_quality_and_health_check(**context: Any) -> dict:
    """Hard checks fail the run (-> DagFailed); soft checks only warn."""
    settings, run_id = _ctx(context)
    day = _day(context)
    validation = _validation(context)
    hard: dict[str, bool] = {}
    soft: dict[str, Any] = {}
    with db.RunLog(settings.pg, "data_quality_and_health_check", run_id, day) as run:
        with db.connect(settings.pg) as conn, conn.cursor() as cur:

            def scalar(sql: str, *params: Any) -> Any:
                cur.execute(sql, params)
                return cur.fetchone()[0]

            patients = scalar("SELECT count(*) FROM patients")
            report_rows = scalar("SELECT count(*) FROM patient_risk_report WHERE sim_day = %s", day)
            hard["report_has_every_patient"] = patients > 0 and report_rows == patients
            ranks = scalar(
                "SELECT count(DISTINCT rank) FROM patient_risk_report WHERE sim_day = %s", day
            )
            hard["report_ranks_unique"] = ranks == report_rows
            if validation.get("status") == "ok":
                loaded = scalar("SELECT count(*) FROM lab_results WHERE sim_day = %s", day)
                hard["lab_rows_loaded_match_validated"] = loaded == validation.get("rows_valid")
                risk_rows = scalar(
                    "SELECT count(*) FROM patient_lab_risk WHERE as_of_sim_day = %s", day
                )
                hard["lab_risk_written"] = risk_rows > 0
                soft["rejected_rows"] = validation.get("rejected")
            else:
                soft["lab_file"] = validation.get("status", "unknown")
            soft["speed_layer_data_age_s"] = scalar(
                "SELECT round(EXTRACT(EPOCH FROM now() - max(last_reading_at))) FROM patient_status"
            )
            cur.execute(
                "SELECT discrepancy_ratio, windows_compared FROM speed_batch_reconciliation "
                "WHERE sim_day = %s",
                (day - 1,),
            )
            recon = cur.fetchone()
            soft["discrepancy_ratio"] = recon[0] if recon else None
        failed = [name for name, ok in hard.items() if not ok]
        run.rows(rows_in=report_rows, hard=hard, soft=soft)
        age = soft["speed_layer_data_age_s"]
        if age is None or float(age) > 120:
            log.warning("speed_layer_stale", sim_day=day, data_age_s=age)
        if soft.get("lab_file") == "quarantined":
            log.warning("lab_file_quarantined_report_uses_previous_labs", sim_day=day)
        if failed:
            log.error("data_quality_failed", sim_day=day, failed=failed, soft=soft)
            raise ValueError(f"data quality checks failed: {failed}")
        log.info("data_quality_passed", sim_day=day, checks=hard, soft=soft)
        return {"hard": hard, "soft": soft}


def archive_file(**context: Any) -> str | None:
    """Move the processed file to ``processed/`` and record the successful run."""
    settings, run_id = _ctx(context)
    day = _day(context)
    validation = _validation(context)
    with db.RunLog(settings.pg, "archive_file", run_id, day) as run:
        target = None
        if validation.get("status") == "ok":
            path = Path(validation["file"])
            if path.exists() and path.parent.resolve() != settings.processed_dir.resolve():
                target = lab_pipeline.move(path, settings.processed_dir)
            else:
                target = settings.processed_dir / path.name
        run.rows(archived_to=str(target) if target else None)
        metrics.push_dag_success(
            settings.pushgateway_url, {"batch_last_processed_sim_day": float(day)}
        )
        log.info("dag_run_succeeded", sim_day=day, archived_to=str(target) if target else None)
        return str(target) if target else None


# ------------------------------------------------------------------------------- callbacks
def on_task_failure(context: dict) -> None:
    """``on_failure_callback``: structured log + failure metric (+ missing-file metric)."""
    settings = BatchSettings.from_env()
    ti = context.get("task_instance") or context.get("ti")
    task_id = getattr(ti, "task_id", "unknown")
    setup_logging(str(context.get("run_id") or "unknown"), settings.base.log_level)
    exc = context.get("exception")
    log.error(
        "task_failed",
        dag_id=DAG_ID,
        task_id=task_id,
        try_number=getattr(ti, "try_number", None),
        error=repr(exc) if exc else None,
    )
    metrics.inc_and_push(
        settings.pushgateway_url, settings.base.state_dir, task_id, "airflow_task_failures_total"
    )
    if task_id == "wait_for_lab_file":
        day = None
        try:
            day = ti.xcom_pull(task_ids="resolve_sim_day")
        except Exception:
            pass
        log.error("lab_file_missing", sim_day=day, timeout_s=settings.sensor_timeout_seconds)
        metrics.inc_and_push(
            settings.pushgateway_url,
            settings.base.state_dir,
            "wait_for_lab_file",
            "lab_file_missing_total",
        )


def on_sla_miss(dag: Any, task_list: str, blocking_task_list: str, slas: list, blocking_tis: list):
    setup_logging("sla", BatchSettings.from_env().base.log_level)
    log.warning(
        "sla_missed",
        dag_id=getattr(dag, "dag_id", DAG_ID),
        tasks=[getattr(s, "task_id", str(s)) for s in slas],
        blocking=[getattr(t, "task_id", str(t)) for t in blocking_tis],
    )
