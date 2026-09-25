"""Streaming source: bedside monitors emitting vital-sign readings to Kafka.

Every ``VITALS_INTERVAL_SECONDS`` (default 2 s) each patient produces one reading
on topic ``vitals.raw``, keyed by ``patient_id`` so that all readings of a patient
land in the same partition (per-patient ordering is what trend calculation needs).

Run::

    python -m simulators.vitals_producer                # against Kafka
    python -m simulators.vitals_producer --dry-run      # print JSON lines, no Kafka needed
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import sys
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from common.config import Settings, env_float
from common.logging_setup import configure_logging, get_logger
from common.schemas import VitalReading
from common.sim_clock import SimClock, iso_utc, load_clock
from simulators import vitals_metrics as metrics
from simulators.faults import FaultConfig, FaultInjector
from simulators.patients import PatientProfile, build_patients
from simulators.vitals_model import VitalsSimulator

log = get_logger("vitals_producer", "ingestion")


class Sink(Protocol):
    """Where readings go: Kafka in production, stdout / a list in tests."""

    def send(self, key: str, value: str) -> None: ...

    def poll(self) -> None: ...

    def flush(self, timeout: float) -> int: ...


class VitalsEmitter:
    """One ``tick`` = one reading per patient, after physiology and fault injection."""

    def __init__(
        self,
        patients: list[PatientProfile],
        clock: SimClock,
        seed: int,
        spike_rate: float,
        faults: FaultInjector,
    ) -> None:
        self.patients = patients
        self.clock = clock
        self.faults = faults
        self.model = VitalsSimulator(patients, seed, spike_rate)
        self._id_rng = random.Random(f"event-ids:{seed}")  # reproducible event_ids
        self._in_episode: dict[str, str] = {}

    def _event_id(self) -> str:
        return str(uuid.UUID(int=self._id_rng.getrandbits(128), version=4))

    def tick(self, now: float) -> list[VitalReading]:
        t = self.clock.elapsed(now)
        sim_day = self.clock.sim_day(now)
        timestamp = iso_utc(now)

        raw_events: list[dict] = []
        for patient in self.patients:
            vitals = self.model.reading(patient, t)
            raw_events.append(
                {
                    "event_id": self._event_id(),
                    "patient_id": patient.patient_id,
                    **vitals,
                    "timestamp": timestamp,
                    "sim_day": sim_day,
                }
            )
            self._track_episode(patient, t)

        emitted = self.faults.release_due(now)
        for event in raw_events:
            emitted.extend(self.faults.process(event, now))
        return [VitalReading(**event) for event in emitted]

    def active_episode_count(self) -> int:
        return len(self._in_episode)

    def _track_episode(self, patient: PatientProfile, t: float) -> None:
        """Log ground-truth episode boundaries so alerts can be checked against them."""
        episode, intensity = patient.active_episode(t)
        was_active = patient.patient_id in self._in_episode
        if episode and intensity > 0.05 and not was_active:
            self._in_episode[patient.patient_id] = episode.kind
            log.info(
                "episode_started",
                patient_id=patient.patient_id,
                kind=episode.kind,
                severity=episode.severity,
            )
        elif was_active and (episode is None or intensity <= 0.02):
            log.info(
                "episode_ended",
                patient_id=patient.patient_id,
                kind=self._in_episode.pop(patient.patient_id),
            )


class StdoutSink:
    def send(self, key: str, value: str) -> None:
        print(value, flush=True)

    def poll(self) -> None:
        pass

    def flush(self, timeout: float) -> int:
        return 0


class KafkaSink:
    """confluent-kafka producer: idempotent, ``acks=all``, retried, with delivery callbacks."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        from confluent_kafka import Producer  # imported lazily so unit tests need no broker

        self.topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "vitals-simulator",
                "enable.idempotence": True,  # no duplicates / reordering from producer retries
                "acks": "all",
                "retries": 2147483647,
                "delivery.timeout.ms": 30000,
                "linger.ms": 50,
                "compression.type": "lz4",
            },
            logger=logging.getLogger("librdkafka"),  # librdkafka logs become JSON lines too
        )

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            metrics.VITALS_PRODUCE_ERRORS.inc()
            log.error("delivery_failed", error=str(err), topic=msg.topic(), key=msg.key())
            return
        metrics.VITALS_PRODUCED.labels(patient=(msg.key() or b"").decode()).inc()

    def send(self, key: str, value: str) -> None:
        for _ in range(50):
            try:
                self._producer.produce(
                    self.topic, key=key, value=value, on_delivery=self._on_delivery
                )
                return
            except BufferError:  # local queue full: let delivery callbacks drain it
                self._producer.poll(0.2)
        metrics.VITALS_PRODUCE_ERRORS.inc()
        log.error("send_dropped_queue_full", key=key)

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self, timeout: float) -> int:
        return self._producer.flush(timeout)


def wait_for_broker(bootstrap_servers: str, topic: str, timeout_s: float = 120.0) -> None:
    """Block until the broker answers and ``topic`` exists (exponential back-off)."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    deadline = time.monotonic() + timeout_s
    delay = 1.0
    while True:
        try:
            metadata = admin.list_topics(timeout=5)
            if topic in metadata.topics and not metadata.topics[topic].error:
                partitions = len(metadata.topics[topic].partitions)
                log.info("broker_ready", topic=topic, partitions=partitions)
                return
            log.warning("topic_not_ready", topic=topic)
        except Exception as exc:  # broker still starting
            log.warning("broker_unavailable", error=str(exc), retry_in_s=delay)
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Kafka topic {topic!r} not available after {timeout_s:.0f}s")
        time.sleep(delay)
        delay = min(delay * 2, 10.0)


def write_ground_truth(directory: str, patients: list[PatientProfile], clock: SimClock) -> Path:
    """Persist the scripted story (who deteriorates when) so alerts can be validated later."""
    out = Path(directory)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "episodes.json"

    def wall(seconds_since_epoch: float) -> str:
        return iso_utc(clock.epoch + seconds_since_epoch)

    document = {
        "sim_epoch": iso_utc(clock.epoch),
        "sim_day_seconds": clock.day_seconds,
        "deteriorating": [
            {
                "patient_id": p.patient_id,
                "episodes": [
                    {
                        "kind": e.kind,
                        "severity": e.severity,
                        "starts_at": wall(e.start_s),
                        "ends_at": wall(e.end_s),
                        "start_sim_day": clock.sim_day(clock.epoch + e.start_s),
                    }
                    for e in p.episodes
                ],
            }
            for p in patients
            if p.episodes
        ],
        "occult_lab_risk": [
            {"patient_id": p.patient_id, "abnormal_labs_from_sim_day": p.occult_from_day}
            for p in patients
            if p.occult_from_day
        ],
    }
    path.write_text(json.dumps(document, indent=2))
    return path


def run(
    sink: Sink,
    emitter: VitalsEmitter,
    interval_s: float,
    stop: threading.Event,
    max_ticks: int | None = None,
    summary_every_s: float = 30.0,
) -> int:
    """Main loop with drift-free scheduling; returns the number of ticks executed."""
    ticks = 0
    produced = 0
    fault_totals: dict[str, int] = {}
    next_tick = time.monotonic()
    last_summary = time.monotonic()

    while not stop.is_set() and (max_ticks is None or ticks < max_ticks):
        now = time.time()
        events = emitter.tick(now)
        for event in events:
            sink.send(event.patient_id, event.model_dump_json())
        sink.poll()

        produced += len(events)
        ticks += 1
        for kind, count in emitter.faults.drain_faults().items():
            metrics.VITALS_FAULTS.labels(type=kind).inc(count)
            fault_totals[kind] = fault_totals.get(kind, 0) + count
        if events:
            metrics.SIMULATOR_LAST_EMIT.set(now)
        metrics.SIMULATOR_SIM_DAY.set(emitter.clock.sim_day(now))
        metrics.SIMULATOR_ACTIVE_EPISODES.set(emitter.active_episode_count())
        metrics.SIMULATOR_LATE_PENDING.set(emitter.faults.pending_late)

        if time.monotonic() - last_summary >= summary_every_s:
            log.info(
                "produce_summary",
                sim_day=emitter.clock.sim_day(now),
                ticks=ticks,
                events_emitted=produced,
                faults_injected=fault_totals,
                late_pending=emitter.faults.pending_late,
            )
            last_summary = time.monotonic()

        next_tick += interval_s
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            stop.wait(sleep_for)
        else:  # fell behind (e.g. laptop suspended): resync instead of bursting
            log.warning("tick_overrun", behind_s=round(-sleep_for, 3))
            next_tick = time.monotonic()
    return ticks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bedside vitals streaming simulator")
    parser.add_argument("--dry-run", action="store_true", help="print JSON to stdout, skip Kafka")
    parser.add_argument("--max-ticks", type=int, default=None, help="stop after N ticks")
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    configure_logging(
        "vitals-simulator",
        settings.log_level,
        stream=sys.stderr if args.dry_run else None,
        default_stage="ingestion",
    )

    clock = load_clock(settings.sim_day_seconds, settings.sim_epoch, settings.sim_epoch_file)
    patients = build_patients(
        settings.num_patients,
        settings.sim_seed,
        settings.sim_day_seconds,
        settings.num_deteriorating,
        settings.num_occult,
    )
    interval = env_float("VITALS_INTERVAL_SECONDS", 2.0)
    faults = FaultConfig.from_env()
    emitter = VitalsEmitter(
        patients,
        clock,
        settings.sim_seed,
        env_float("SPIKE_RATE", 0.01),
        FaultInjector(faults, settings.sim_seed),
    )

    truth = write_ground_truth(settings.ground_truth_dir, patients, clock)
    log.info(
        "producer_started",
        patients=len(patients),
        interval_s=interval,
        sim_day_seconds=clock.day_seconds,
        sim_epoch=iso_utc(clock.epoch),
        current_sim_day=clock.sim_day(),
        topic=settings.vitals_topic,
        fault_config=faults.__dict__,
        ground_truth=str(truth),
        started_at=datetime.now(UTC).isoformat(),
    )

    from prometheus_client import start_http_server

    start_http_server(int(env_float("METRICS_PORT", 8001)))

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    if args.dry_run:
        sink: Sink = StdoutSink()
    else:
        wait_for_broker(settings.kafka_bootstrap_servers, settings.vitals_topic)
        sink = KafkaSink(settings.kafka_bootstrap_servers, settings.vitals_topic)

    ticks = run(sink, emitter, interval, stop, args.max_ticks)
    remaining = sink.flush(15)
    log.info("producer_stopped", ticks=ticks, unflushed=remaining)
    return 0


if __name__ == "__main__":
    sys.exit(main())
