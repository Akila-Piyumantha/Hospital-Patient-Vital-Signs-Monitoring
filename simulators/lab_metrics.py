"""Prometheus metrics of the lab generator (served on METRICS_PORT, default 8002)."""

from __future__ import annotations

from prometheus_client import Counter, Gauge

LAB_FILES_DROPPED = Counter(
    "lab_files_dropped_total", "Daily lab files written to the landing zone"
)
LAB_ROWS_GENERATED = Counter("lab_rows_generated_total", "Lab result rows written to landing files")
LAB_FILES_SKIPPED = Counter(
    "lab_files_skipped_total", "Lab files deliberately not dropped", ["reason"]
)
LAB_FILES_FAULTY = Counter(
    "lab_files_faulty_total", "Lab files dropped with injected defects", ["kind"]
)
LAB_LAST_DAY = Gauge("lab_generator_last_day", "Sim day of the last lab file handled")
