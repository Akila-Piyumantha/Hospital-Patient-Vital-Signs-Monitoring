"""Configuration of the batch layer (everything from the environment, see ``.env.example``)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from common.config import Settings, env_float, env_int, env_str
from common.sim_clock import SimClock, parse_epoch

DAG_ID = "daily_lab_risk_report"


@dataclass(frozen=True)
class BatchSettings:
    base: Settings
    lake_dir: Path
    reports_dir: Path
    pushgateway_url: str
    # The batch job recomputes day N-1 only once the speed layer can have finished it: the day
    # has ended, plus the streaming watermark, plus one trigger (late data and the last lake write).
    settle_seconds: float
    sensor_timeout_seconds: float
    sensor_poke_seconds: float
    # Share of the day (at its end) whose averages give the "end of day" NEWS used by the report.
    end_of_day_fraction: float
    sparkline_buckets: int
    spark_master: str
    spark_driver_memory: str
    discrepancy_alert_ratio: float

    @property
    def landing_dir(self) -> Path:
        return Path(self.base.landing_dir)

    @property
    def processed_dir(self) -> Path:
        return self.landing_dir / "processed"

    @property
    def quarantine_dir(self) -> Path:
        return self.landing_dir / "quarantine"

    @property
    def pg(self) -> dict:
        b = self.base
        return {
            "host": b.postgres_host,
            "port": b.postgres_port,
            "user": b.postgres_user,
            "password": b.postgres_password,
            "dbname": b.postgres_db,
        }

    @classmethod
    def from_env(cls) -> BatchSettings:
        base = Settings.from_env()
        return cls(
            base=base,
            lake_dir=Path(env_str("LAKE_DIR", "data/lake/vitals")),
            reports_dir=Path(env_str("REPORTS_DIR", "data/reports")),
            pushgateway_url=env_str("PUSHGATEWAY_URL", "localhost:9091"),
            settle_seconds=env_float("BATCH_SETTLE_SECONDS", 75.0),
            sensor_timeout_seconds=env_float(
                "LAB_SENSOR_TIMEOUT_SECONDS", 0.8 * base.sim_day_seconds
            ),
            sensor_poke_seconds=env_float("LAB_SENSOR_POKE_SECONDS", 10.0),
            end_of_day_fraction=env_float("REPORT_END_OF_DAY_FRACTION", 0.25),
            sparkline_buckets=env_int("REPORT_SPARKLINE_BUCKETS", 24),
            spark_master=env_str("BATCH_SPARK_MASTER", "local[2]"),
            spark_driver_memory=env_str("BATCH_SPARK_DRIVER_MEMORY", "768m"),
            discrepancy_alert_ratio=env_float("DISCREPANCY_ALERT_RATIO", 0.10),
        )


def read_clock(settings: Settings) -> SimClock:
    """The shared simulated clock, **read-only**.

    ``common.sim_clock.load_clock`` creates the epoch file when it is missing; the batch layer
    must never do that (a DAG run before the simulators would pin a wrong epoch), so it fails
    instead and the task is retried.
    """
    if settings.sim_epoch:
        return SimClock(parse_epoch(settings.sim_epoch), settings.sim_day_seconds)
    path = Path(settings.sim_epoch_file)
    try:
        text = path.read_text().strip()
    except OSError as exc:
        raise RuntimeError(
            f"sim epoch file {path} not found - are the simulators running?"
        ) from exc
    if not text:
        raise RuntimeError(f"sim epoch file {path} is empty")
    return SimClock(float(text), settings.sim_day_seconds)
