"""Environment-driven configuration.

Every tunable (topic names, simulated-day length, fault rates, ...) comes from an
environment variable with a sane default, so the stack runs with zero setup and
is tuned through ``.env`` / ``docker-compose.yml`` only. An *empty* variable is
treated as unset (Compose passes ``${VAR:-}`` as an empty string).
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _raw(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return None
    return value.strip()


def env_str(name: str, default: str) -> str:
    return _raw(name) or default


def env_int(name: str, default: int) -> int:
    value = _raw(name)
    return default if value is None else int(value)


def env_float(name: str, default: float) -> float:
    value = _raw(name)
    return default if value is None else float(value)


def env_bool(name: str, default: bool) -> bool:
    value = _raw(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def env_int_set(name: str) -> frozenset[int]:
    """Parse ``"3,5, 7"`` into ``frozenset({3, 5, 7})``; unset -> empty set."""
    value = _raw(name)
    if value is None:
        return frozenset()
    return frozenset(int(part) for part in value.split(",") if part.strip())


@dataclass(frozen=True)
class Settings:
    """Settings shared by all Python services in this repository."""

    # Simulated clock (contract: 1 simulated day = SIM_DAY_SECONDS real seconds)
    sim_day_seconds: float
    sim_epoch: str | None
    sim_epoch_file: str

    # Cohort
    sim_seed: int
    num_patients: int
    num_deteriorating: int
    num_occult: int

    # Kafka
    kafka_bootstrap_servers: str
    vitals_topic: str
    dlq_topic: str

    # Files
    landing_dir: str
    state_dir: str
    ground_truth_dir: str

    # Postgres
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_db: str

    log_level: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            sim_day_seconds=env_float("SIM_DAY_SECONDS", 300.0),
            sim_epoch=_raw("SIM_EPOCH"),
            sim_epoch_file=env_str("SIM_EPOCH_FILE", "data/state/sim_epoch"),
            sim_seed=env_int("SIM_SEED", 42),
            num_patients=env_int("NUM_PATIENTS", 20),
            num_deteriorating=env_int("NUM_DETERIORATING", 4),
            num_occult=env_int("NUM_OCCULT", 2),
            kafka_bootstrap_servers=env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            vitals_topic=env_str("VITALS_TOPIC", "vitals.raw"),
            dlq_topic=env_str("DLQ_TOPIC", "vitals.dlq"),
            landing_dir=env_str("LANDING_DIR", "data/landing/labs"),
            state_dir=env_str("STATE_DIR", "data/state"),
            ground_truth_dir=env_str("GROUND_TRUTH_DIR", "data/ground_truth"),
            postgres_host=env_str("POSTGRES_HOST", "localhost"),
            postgres_port=env_int("POSTGRES_PORT", 5432),
            postgres_user=env_str("POSTGRES_USER", "hospital"),
            postgres_password=env_str("POSTGRES_PASSWORD", "hospital"),
            postgres_db=env_str("POSTGRES_DB", "hospital"),
            log_level=env_str("LOG_LEVEL", "INFO").upper(),
        )
