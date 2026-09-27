"""Settings of the streaming job; every value is an environment variable with a default."""

from __future__ import annotations

from dataclasses import dataclass

from common.config import Settings, env_int, env_str


@dataclass(frozen=True)
class StreamSettings:
    base: Settings

    spark_master: str
    shuffle_partitions: int
    trigger_interval: str
    starting_offsets: str
    max_offsets_per_trigger: int

    window_duration: str
    window_slide: str
    watermark: str  # speed-layer lateness bound (windows, dedupe)
    lake_watermark: str  # dedupe horizon for the lake: late events must still reach Parquet
    min_window_readings: int  # windows with fewer readings are ignored by trend/sustained logic

    checkpoint_dir: str
    lake_dir: str
    lake_staging_dir: str

    consumer_group: str  # offsets are committed here so kafka-exporter can report lag
    metrics_port: int

    @property
    def jdbc_url(self) -> str:
        b = self.base
        return f"jdbc:postgresql://{b.postgres_host}:{b.postgres_port}/{b.postgres_db}"

    @property
    def pg(self) -> dict:
        """psycopg2 connection kwargs (plain dict: shipped to executors inside closures)."""
        b = self.base
        return {
            "host": b.postgres_host,
            "port": b.postgres_port,
            "user": b.postgres_user,
            "password": b.postgres_password,
            "dbname": b.postgres_db,
        }

    @classmethod
    def from_env(cls) -> StreamSettings:
        return cls(
            base=Settings.from_env(),
            spark_master=env_str("SPARK_MASTER", "local[2]"),
            # = Kafka partitions: 200 (Spark default) would mean 200 tiny state-store tasks/batch
            shuffle_partitions=env_int("SPARK_SHUFFLE_PARTITIONS", 3),
            trigger_interval=env_str("STREAM_TRIGGER_INTERVAL", "5 seconds"),
            starting_offsets=env_str("STREAM_STARTING_OFFSETS", "latest"),
            max_offsets_per_trigger=env_int("STREAM_MAX_OFFSETS_PER_TRIGGER", 5000),
            window_duration=env_str("STREAM_WINDOW_DURATION", "2 minutes"),
            window_slide=env_str("STREAM_WINDOW_SLIDE", "30 seconds"),
            watermark=env_str("STREAM_WATERMARK", "1 minute"),
            lake_watermark=env_str("LAKE_DEDUP_WATERMARK", "10 minutes"),
            min_window_readings=env_int("STREAM_MIN_WINDOW_READINGS", 20),
            checkpoint_dir=env_str("CHECKPOINT_DIR", "data/checkpoints"),
            lake_dir=env_str("LAKE_DIR", "data/lake/vitals"),
            lake_staging_dir=env_str("LAKE_STAGING_DIR", "data/lake/_staging"),
            consumer_group=env_str("STREAM_CONSUMER_GROUP", "spark-speed-layer"),
            metrics_port=env_int("METRICS_PORT", 8003),
        )
