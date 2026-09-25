"""Simulated clock.

The whole pipeline shares one notion of "simulated day": ``SIM_DAY_SECONDS`` real
seconds (default 300 = 5 minutes) make one day, counted from a common epoch:

    sim_day = floor((now - epoch) / SIM_DAY_SECONDS) + 1        # day 1 starts at the epoch

Every service (vitals simulator, lab generator, Spark, Airflow) must derive
``sim_day`` from the same epoch. The epoch comes from ``SIM_EPOCH`` (unix seconds
or ISO-8601) or, when unset, from a small file on the shared ``data`` volume that
the first service to start creates atomically. Delete the file (``make reset``)
to restart simulated time at day 1.
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True)
class SimClock:
    epoch: float  # unix seconds at which simulated day 1 starts
    day_seconds: float = 300.0

    def elapsed(self, now: float | None = None) -> float:
        """Real seconds since the epoch (negative before the epoch)."""
        return (time.time() if now is None else now) - self.epoch

    def sim_day(self, now: float | None = None) -> int:
        """1-based simulated day number; day 0 is the (virtual) day before the epoch."""
        return math.floor(self.elapsed(now) / self.day_seconds) + 1

    def day_start(self, day: int) -> float:
        """Unix time at which simulated ``day`` starts."""
        return self.epoch + (day - 1) * self.day_seconds

    def day_end(self, day: int) -> float:
        return self.day_start(day + 1)

    def seconds_into_day(self, now: float | None = None) -> float:
        return self.elapsed(now) % self.day_seconds

    def seconds_until_next_day(self, now: float | None = None) -> float:
        return self.day_seconds - self.seconds_into_day(now)


def parse_epoch(value: str) -> float:
    """Accept unix seconds (``1767225600``) or ISO-8601 (``2026-01-01T00:00:00Z``)."""
    try:
        return float(value)
    except ValueError:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _read_or_create_epoch(path: Path, now: float) -> float:
    """Return the epoch stored in ``path``, creating it (atomically) if absent.

    ``O_EXCL`` makes creation race-free when the vitals simulator and the lab
    generator start at the same moment; the loser simply reads the winner's value.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        pass
    else:
        stored = round(now, 3)  # the creator must return exactly what readers will parse
        with os.fdopen(fd, "w") as handle:
            handle.write(f"{stored:.3f}")
        return stored

    for _ in range(100):  # the creator may not have written the value yet
        text = path.read_text().strip()
        if text:
            return float(text)
        time.sleep(0.05)
    raise RuntimeError(f"sim epoch file {path} exists but is empty")


def load_clock(
    day_seconds: float,
    epoch: str | None = None,
    epoch_file: str | os.PathLike[str] = "data/state/sim_epoch",
    now: float | None = None,
) -> SimClock:
    """Build the shared clock from ``epoch`` if given, else from ``epoch_file``."""
    if epoch:
        return SimClock(parse_epoch(epoch), day_seconds)
    now = time.time() if now is None else now
    return SimClock(_read_or_create_epoch(Path(epoch_file), now), day_seconds)


def iso_utc(unix_seconds: float) -> str:
    """Contract timestamp format: ``2026-03-01T10:15:02.123Z`` (UTC, millisecond precision)."""
    dt = datetime.fromtimestamp(unix_seconds, tz=UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
