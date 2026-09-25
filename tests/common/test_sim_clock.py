import multiprocessing

import pytest

from common.sim_clock import SimClock, iso_utc, load_clock, parse_epoch


def test_sim_day_boundaries():
    clock = SimClock(epoch=1000.0, day_seconds=300)
    assert clock.sim_day(1000.0) == 1  # day 1 starts exactly at the epoch
    assert clock.sim_day(1299.9) == 1
    assert clock.sim_day(1300.0) == 2
    assert clock.sim_day(1000.0 + 5 * 300 + 1) == 6
    assert clock.sim_day(999.0) == 0  # virtual day 0 before the epoch


def test_day_start_end_and_offsets():
    clock = SimClock(epoch=1000.0, day_seconds=300)
    assert clock.day_start(1) == 1000.0
    assert clock.day_start(3) == 1600.0
    assert clock.day_end(3) == 1900.0
    assert clock.seconds_into_day(1350.0) == pytest.approx(50.0)
    assert clock.seconds_until_next_day(1350.0) == pytest.approx(250.0)


def test_parse_epoch_accepts_unix_and_iso():
    assert parse_epoch("1767225600") == 1767225600.0
    assert parse_epoch("2026-01-01T00:00:00Z") == 1767225600.0


def test_iso_utc_contract_format():
    assert iso_utc(1767225600.123) == "2026-01-01T00:00:00.123Z"


def test_load_clock_uses_explicit_epoch(tmp_path):
    clock = load_clock(60, epoch="500", epoch_file=tmp_path / "e")
    assert clock.epoch == 500.0 and clock.day_seconds == 60
    assert not (tmp_path / "e").exists()  # explicit epoch must not touch the file


def test_load_clock_creates_then_reuses_epoch_file(tmp_path):
    path = tmp_path / "state" / "sim_epoch"
    first = load_clock(300, epoch_file=path, now=1234.5)
    second = load_clock(300, epoch_file=path, now=9999.0)  # later start must see the same epoch
    assert first.epoch == second.epoch == 1234.5


def _start(path, out):
    out.put(load_clock(300, epoch_file=path).epoch)


def test_concurrent_start_agrees_on_epoch(tmp_path):
    """Vitals simulator and lab generator may boot simultaneously: one epoch wins."""
    path = tmp_path / "epoch"
    out = multiprocessing.Queue()
    procs = [multiprocessing.Process(target=_start, args=(path, out)) for _ in range(4)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(10)
    assert len({out.get(timeout=5) for _ in procs}) == 1
