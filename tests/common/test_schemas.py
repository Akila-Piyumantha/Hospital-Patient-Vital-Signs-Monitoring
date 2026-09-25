import json

from common.schemas import LAB_COLUMNS, VITAL_FIELDS, VITALS_SPARK_DDL, LabRow, VitalReading


def test_vital_reading_contract_roundtrip():
    reading = VitalReading(
        event_id="e1",
        patient_id="P001",
        heart_rate=78,
        spo2=97,
        systolic_bp=121,
        diastolic_bp=79,
        temperature=36.8,
        timestamp="2026-03-01T10:15:02.123Z",
        sim_day=3,
    )
    payload = json.loads(reading.model_dump_json())
    assert set(payload) == {
        "event_id", "patient_id", *VITAL_FIELDS, "timestamp", "sim_day",
    }  # fmt: skip
    assert VitalReading(**payload) == reading


def test_null_and_out_of_range_values_are_representable():
    """Faults must serialise; rejecting them is the speed layer's job (-> DLQ)."""
    reading = VitalReading(
        event_id="e", patient_id="P1", heart_rate=None, spo2=150,
        timestamp="t", sim_day=1,
    )  # fmt: skip
    assert json.loads(reading.model_dump_json())["heart_rate"] is None


def test_spark_ddl_lists_every_contract_field():
    for name in ("event_id", "patient_id", *VITAL_FIELDS, "timestamp", "sim_day"):
        assert name in VITALS_SPARK_DDL


def test_lab_row_matches_contract_columns():
    row = LabRow(
        patient_id="P001", test_type="lactate", result_value=3.1,
        reference_range="0.5-2.0", collected_at="2026-03-01T08:30:00Z",
    )  # fmt: skip
    assert tuple(row.model_dump()) == LAB_COLUMNS
