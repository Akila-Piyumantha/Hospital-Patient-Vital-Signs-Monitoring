"""End-to-end smoke test of the running stack.

    python scripts/e2e_smoke.py            # platform + ingestion checks (Member A's scope)
    python scripts/e2e_smoke.py --full     # + speed layer, batch layer, serving (Members B/C)
    python scripts/e2e_smoke.py --wait 180 # keep retrying for up to 180 s while the stack warms up

Exit code 0 = every non-skipped check passed. Needs only the standard library; Postgres checks
use psycopg2 when installed (``pip install -r requirements-dev.txt``) and are skipped otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROM = os.environ.get("PROM_URL", "http://localhost:9090")
API = os.environ.get("API_URL", "http://localhost:8000")
PG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "user": os.environ.get("POSTGRES_USER", "hospital"),
    "password": os.environ.get("POSTGRES_PASSWORD", "hospital"),
    "dbname": os.environ.get("POSTGRES_DB", "hospital"),
}
EXPECTED_PATIENTS = int(os.environ.get("NUM_PATIENTS", "20"))
EXPECTED_PARTITIONS = int(os.environ.get("VITALS_PARTITIONS", "3"))


class Skip(Exception):
    pass


def http_json(url: str, timeout: float = 5.0):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read())


def prom_query(expr: str) -> list[dict]:
    url = f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    body = http_json(url)
    if body.get("status") != "success":
        raise RuntimeError(f"prometheus query failed: {body}")
    return body["data"]["result"]


def pg_scalar(sql: str):
    try:
        import psycopg2
    except ImportError as exc:
        raise Skip("psycopg2 not installed") from exc
    conn = psycopg2.connect(connect_timeout=5, **PG)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]
    finally:
        conn.close()


# -- checks: each returns a short detail string or raises AssertionError / Skip ------------


def check_targets_up() -> str:
    up = {r["metric"]["job"]: r["value"][1] for r in prom_query("up")}
    required = ["vitals-simulator", "lab-generator", "kafka-exporter", "alertmanager"]
    down = [job for job in required if up.get(job) != "1"]
    assert not down, f"targets down: {down}"
    return f"{len(up)} targets known, required all up"


def check_kafka_flow() -> str:
    expr = 'kafka_topic_partition_current_offset{topic="vitals.raw"}'

    def offsets() -> dict[str, float]:
        return {r["metric"]["partition"]: float(r["value"][1]) for r in prom_query(expr)}

    first = offsets()
    assert len(first) == EXPECTED_PARTITIONS, f"expected {EXPECTED_PARTITIONS} partitions: {first}"
    time.sleep(12)
    second = offsets()
    grown = {p: second[p] - first[p] for p in first}
    assert all(delta > 0 for delta in grown.values()), f"a partition received nothing: {grown}"
    return f"messages in 12s per partition: {grown}"


def check_patients_seeded() -> str:
    count = pg_scalar("SELECT count(*) FROM patients")
    assert count == EXPECTED_PATIENTS, f"patients table has {count} rows"
    return f"{count} patients"


def check_lab_files() -> str:
    files = sorted((ROOT / "data" / "landing").rglob("labs_day_*.csv"))
    assert files, "no labs_day_*.csv anywhere under data/landing"
    header = files[-1].read_text().splitlines()[0]
    return f"{len(files)} lab file(s), latest {files[-1].name}, header: {header}"


def check_ground_truth() -> str:
    path = ROOT / "data" / "ground_truth" / "episodes.json"
    doc = json.loads(path.read_text())
    return (
        f"{len(doc['deteriorating'])} deteriorating, {len(doc['occult_lab_risk'])} occult patients"
    )


def check_alert_pipeline() -> str:
    rules = http_json(f"{PROM}/api/v1/rules")
    names = {r["name"] for g in rules["data"]["groups"] for r in g["rules"]}
    required = {"NoVitalsData", "DlqRateHigh", "LabFileMissing", "ConsumerLagHigh"}
    assert required <= names, f"missing alert rules: {required - names}"
    am = http_json("http://localhost:9093/api/v2/status")
    assert am["cluster"]["status"] == "ready"
    return f"{len(names)} rules loaded, Alertmanager ready"


# full-stack checks (Members B/C)
def check_speed_layer() -> str:
    fresh = pg_scalar(
        "SELECT count(*) FROM patient_status WHERE last_reading_at > now() - interval '30 seconds'"
    )
    assert fresh > 0, "patient_status has no fresh rows (is spark-streaming running?)"
    windows = pg_scalar("SELECT count(*) FROM vitals_window")
    assert windows > 0, "vitals_window empty"
    return f"{fresh} patients updated in the last 30 s, {windows} windows"


def check_alerts_raised() -> str:
    count = pg_scalar("SELECT count(*) FROM alerts")
    assert count > 0, "no patient alert raised yet (deterioration starts ~sim day 2)"
    return f"{count} patient alerts"


def check_batch_layer() -> str:
    labs = pg_scalar("SELECT count(*) FROM lab_results")
    report = pg_scalar("SELECT count(*) FROM patient_risk_report")
    assert labs > 0 and report > 0, f"lab_results={labs}, patient_risk_report={report}"
    return f"{labs} lab results, {report} report rows"


def check_api() -> str:
    assert http_json(f"{API}/health")
    summary = http_json(f"{API}/api/ward/summary")
    return f"ward summary keys: {sorted(summary)[:6]}"


BASE: list[tuple[str, Callable[[], str]]] = [
    ("Prometheus targets up", check_targets_up),
    ("Kafka receives data on every partition", check_kafka_flow),
    ("Patients seeded in Postgres", check_patients_seeded),
    ("Daily lab file dropped", check_lab_files),
    ("Ground-truth episodes written", check_ground_truth),
    ("Alert rules loaded, Alertmanager ready", check_alert_pipeline),
]
FULL: list[tuple[str, Callable[[], str]]] = [
    ("Speed layer updating patient_status", check_speed_layer),
    ("Patient alerts raised", check_alerts_raised),
    ("Batch layer produced risk report", check_batch_layer),
    ("API healthy and serving ward summary", check_api),
]


def run_check(name: str, fn: Callable[[], str], wait: float) -> str:
    deadline = time.monotonic() + wait
    while True:
        try:
            detail = fn()
            print(f"  PASS  {name}: {detail}")
            return "pass"
        except Skip as skip:
            print(f"  SKIP  {name}: {skip}")
            return "skip"
        except (AssertionError, OSError, urllib.error.URLError, RuntimeError, KeyError) as exc:
            if time.monotonic() >= deadline:
                print(f"  FAIL  {name}: {type(exc).__name__}: {exc}")
                return "fail"
            time.sleep(5)
        except Exception as exc:  # e.g. psycopg2.Error - table/DB not ready yet
            if time.monotonic() >= deadline:
                print(f"  FAIL  {name}: {type(exc).__name__}: {exc}")
                return "fail"
            time.sleep(5)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--full", action="store_true", help="include speed/batch/serving checks")
    parser.add_argument("--wait", type=float, default=0, help="retry each check for N seconds")
    args = parser.parse_args()

    checks = BASE + (FULL if args.full else [])
    print(f"E2E smoke test ({len(checks)} checks)")
    results = [run_check(name, fn, args.wait) for name, fn in checks]
    failed = results.count("fail")
    print(f"\n{results.count('pass')} passed, {results.count('skip')} skipped, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
