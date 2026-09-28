"""Batch-layer metrics, pushed to the Prometheus Pushgateway (contract 4.7, docs/CHANGELOG.md).

Airflow tasks are short-lived processes, so they cannot be scraped; they push instead, grouped by
``job="daily_lab_risk_report"`` and ``task=<task id>`` (``pushadd`` replaces only the metrics
of that group, so tasks never overwrite each other's values). Every series carries
``dag_id="daily_lab_risk_report"``.

Counters (``lab_files_ingested_total``, ``lab_file_missing_total``, ...) must keep growing across
processes for ``increase()`` in the alert rules to work, so their running totals are kept in a
small JSON file on the shared data volume (``data/state/batch_metrics.json``).

Pushing is best effort: a Pushgateway outage is logged and never fails a pipeline task.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from batch.settings import DAG_ID
from common.logging_setup import get_logger

log = get_logger("batch.metrics", "orchestration")

HELP = {
    "lab_files_ingested_total": "Lab files validated and loaded by the DAG",
    "lab_file_missing_total": "Lab files that did not arrive before the sensor timeout",
    "lab_files_quarantined_total": "Lab files rejected as a whole (bad schema / unreadable)",
    "lab_rows_quarantined_total": "Lab rows rejected by validation",
    "airflow_task_failures_total": "Failed Airflow task attempts of the batch DAG",
    "airflow_dag_last_success_timestamp": "Unix time of the last successful run of the DAG",
    "speed_batch_discrepancy_ratio": "Share of readings the speed layer counted differently",
    "batch_vitals_readings": "Distinct readings of the last recomputed day in the lake",
    "batch_last_processed_sim_day": "Last lab/file day fully processed by the DAG",
}


class CounterStore:
    """Monotonic counters per task, persisted in a JSON file (atomic replace on every update)."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def read(self) -> dict[str, dict[str, float]]:
        try:
            doc = json.loads(self.path.read_text())
            return {t: {n: float(v) for n, v in c.items()} for t, c in doc.items()}
        except (OSError, ValueError, AttributeError):
            return {}

    def inc(self, task: str, name: str, amount: float = 1.0) -> dict[str, float]:
        """Add ``amount`` and return all counter totals of ``task``."""
        doc = self.read()
        counters = doc.setdefault(task, {})
        counters[name] = counters.get(name, 0.0) + amount
        self._write(doc)
        return counters

    def _write(self, doc: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, sort_keys=True))
        os.replace(tmp, self.path)


def counter_store(state_dir: str) -> CounterStore:
    return CounterStore(Path(state_dir) / "batch_metrics.json")


def build_registry(gauges: dict[str, float], counters: dict[str, float]):
    from prometheus_client import CollectorRegistry, Counter, Gauge

    registry = CollectorRegistry()
    for name, value in gauges.items():
        Gauge(name, HELP.get(name, name), ["dag_id"], registry=registry).labels(DAG_ID).set(value)
    for name, total in counters.items():
        counter = Counter(
            name.removesuffix("_total"), HELP.get(name, name), ["dag_id"], registry=registry
        )
        counter.labels(DAG_ID).inc(total)
    return registry


def push(
    gateway: str,
    task: str,
    gauges: dict[str, float] | None = None,
    counters: dict[str, float] | None = None,
) -> bool:
    """Push gauge values and counter totals as group ``task``; False (logged) on failure."""
    from prometheus_client import pushadd_to_gateway

    registry = build_registry(gauges or {}, counters or {})
    try:
        pushadd_to_gateway(
            gateway, job=DAG_ID, grouping_key={"task": task}, registry=registry, timeout=5
        )
        return True
    except Exception as exc:
        log.warning("metrics_push_failed", task=task, gateway=gateway, error=repr(exc))
        return False


def inc_and_push(gateway: str, state_dir: str, task: str, name: str, amount: float = 1.0) -> bool:
    """Increment a persistent counter of ``task`` and push that task's counters."""
    try:
        counters = counter_store(state_dir).inc(task, name, amount)
    except OSError as exc:
        log.warning("metrics_state_unwritable", task=task, error=repr(exc))
        return False
    return push(gateway, task, counters=counters)


def ensure_counters(gateway: str, state_dir: str, counters: dict[str, tuple[str, ...]]) -> bool:
    """Push every alerting counter (``{task: (name, ...)}``) at least once, at 0 if never hit.

    ``increase()`` cannot see a counter's first increment when the series did not exist before
    (absent -> 1), so ``LabFileMissing`` would miss the first missing file without this.
    """
    ok = True
    for task, names in counters.items():
        try:
            totals = counter_store(state_dir).inc(task, names[0], 0.0)
            for name in names[1:]:
                totals = counter_store(state_dir).inc(task, name, 0.0)
        except OSError as exc:
            log.warning("metrics_state_unwritable", task=task, error=repr(exc))
            return False
        ok &= push(gateway, task, counters=totals)
    return ok


def push_dag_success(gateway: str, gauges: dict[str, float] | None = None) -> bool:
    return push(
        gateway,
        "dag_success",
        gauges={"airflow_dag_last_success_timestamp": time.time(), **(gauges or {})},
    )
