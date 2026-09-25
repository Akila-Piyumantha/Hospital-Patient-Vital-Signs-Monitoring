import json
import threading

from common.sim_clock import SimClock
from simulators.faults import FaultConfig, FaultInjector
from simulators.patients import build_patients
from simulators.vitals_producer import VitalsEmitter, run, write_ground_truth

PATIENTS = build_patients(20, seed=42, day_seconds=300)


class ListSink:
    def __init__(self):
        self.sent: list[tuple[str, str]] = []

    def send(self, key, value):
        self.sent.append((key, value))

    def poll(self):
        pass

    def flush(self, timeout):
        return 0


def make_emitter(profile="off", epoch=1000.0, seed=42):
    clock = SimClock(epoch=epoch, day_seconds=300)
    return VitalsEmitter(
        PATIENTS, clock, seed, 0.01, FaultInjector(FaultConfig.from_profile(profile), seed)
    )


def test_one_tick_emits_one_reading_per_patient_with_contract_fields():
    events = make_emitter().tick(now=1010.0)
    assert [e.patient_id for e in events] == [p.patient_id for p in PATIENTS]
    first = json.loads(events[0].model_dump_json())
    assert set(first) == {"event_id", "patient_id", "heart_rate", "spo2", "systolic_bp",
                          "diastolic_bp", "temperature", "timestamp", "sim_day"}  # fmt: skip
    assert first["timestamp"] == "1970-01-01T00:16:50.000Z"
    assert first["sim_day"] == 1


def test_sim_day_advances_with_the_clock():
    emitter = make_emitter()
    assert emitter.tick(now=1000.0 + 299)[0].sim_day == 1
    assert emitter.tick(now=1000.0 + 301)[0].sim_day == 2


def test_event_ids_are_unique_and_reproducible():
    a = [e.event_id for _ in range(3) for e in make_emitter().tick(1000.0)]
    ids = [
        e.event_id
        for e in (lambda em: [x for i in range(5) for x in em.tick(1000.0 + i)])(make_emitter())
    ]
    assert len(set(ids)) == len(ids) == 100
    assert a[:20] == [e.event_id for e in make_emitter().tick(1000.0)]


def test_faults_flow_through_the_emitter():
    emitter = make_emitter("chaos")
    total = []
    for i in range(50):
        total += emitter.tick(1000.0 + i * 2)
    kinds = emitter.faults.drain_faults()
    assert kinds["null"] > 0 and kinds["duplicate"] > 0 and kinds["late"] > 0
    ids = [e.event_id for e in total]
    assert len(ids) > len(set(ids))  # duplicates share an event_id


def test_run_loop_keys_messages_by_patient_and_stops_at_max_ticks():
    sink = ListSink()
    ticks = run(sink, make_emitter(epoch=1.0), interval_s=0.0, stop=threading.Event(), max_ticks=3)
    assert ticks == 3 and len(sink.sent) == 60
    for key, value in sink.sent:
        assert json.loads(value)["patient_id"] == key


def test_run_loop_stops_promptly_on_signal():
    sink = ListSink()
    stop = threading.Event()
    stop.set()
    assert run(sink, make_emitter(), interval_s=2.0, stop=stop) == 0
    assert sink.sent == []


def test_ground_truth_file_lists_scripted_patients(tmp_path):
    clock = SimClock(epoch=1_800_000_000.0, day_seconds=300)
    path = write_ground_truth(str(tmp_path), PATIENTS, clock)
    doc = json.loads(path.read_text())
    assert len(doc["deteriorating"]) == 4 and len(doc["occult_lab_risk"]) == 2
    assert doc["deteriorating"][0]["episodes"][0]["kind"] in {"sepsis", "respiratory"}
    assert doc["sim_day_seconds"] == 300
