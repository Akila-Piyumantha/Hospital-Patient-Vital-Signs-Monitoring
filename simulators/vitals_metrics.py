"""Prometheus metrics of the vitals simulator (served on METRICS_PORT, default 8001).

Kept apart from the lab generator's metrics: each process exposes only its own series,
otherwise every scrape target would also report the other's metrics as zeros.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge

VITALS_PRODUCED = Counter(
    "vitals_produced_total", "Vital readings acknowledged by Kafka", ["patient"]
)
VITALS_PRODUCE_ERRORS = Counter(
    "vitals_produce_errors_total", "Vital readings that failed delivery to Kafka"
)
VITALS_FAULTS = Counter(
    "vitals_faults_injected_total", "Faults deliberately injected into the stream", ["type"]
)
SIMULATOR_LAST_EMIT = Gauge(
    "simulator_last_emit_timestamp_seconds", "Unix time of the last reading handed to Kafka"
)
SIMULATOR_SIM_DAY = Gauge("simulator_sim_day", "Current simulated day number")
SIMULATOR_ACTIVE_EPISODES = Gauge(
    "simulator_active_episodes", "Patients currently inside a scripted deterioration episode"
)
SIMULATOR_LATE_PENDING = Gauge(
    "simulator_late_events_pending", "Late events currently held back by the fault injector"
)
