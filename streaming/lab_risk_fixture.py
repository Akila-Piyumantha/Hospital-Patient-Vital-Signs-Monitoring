"""Stand-in for Member C's ``compute_lab_risk`` task: fixture for the lab feedback loop (B8).

Until the Airflow DAG exists, this loads one day's lab file into ``patient_lab_risk`` with
the shared scoring rules, so the speed layer's lab join can be demonstrated:

    docker compose exec spark-streaming python -m streaming.lab_risk_fixture --day 3
    python -m streaming.lab_risk_fixture --day 3 --dry-run   # print, write nothing

Rows it cannot parse are skipped (the real DAG quarantines them). Idempotent: upsert on
(patient_id, as_of_sim_day). Retire it once C3 is merged.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from common import scoring
from common.config import Settings


def lab_risk_from_csv(path: Path) -> dict[str, tuple[int, list[str]]]:
    """patient_id -> (lab_risk_points, ["lactate:high", ...]) for one lab file."""
    abnormal: dict[str, list[tuple[str, str]]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            try:
                low, high = scoring.parse_reference_range(row["reference_range"])
                value = float(row["result_value"])
                patient, test = row["patient_id"].strip(), row["test_type"].strip().lower()
            except (KeyError, ValueError, AttributeError):
                continue
            abnormal.setdefault(patient, [])
            direction = scoring.abnormal_direction(value, low, high)
            if direction:
                abnormal[patient].append((test, direction))
    return {
        pid: (scoring.lab_risk_points(tests), sorted({f"{t}:{d}" for t, d in tests}))
        for pid, tests in abnormal.items()
    }


def main(argv: list[str] | None = None) -> int:
    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--day", type=int, required=True, help="sim day of the lab file")
    parser.add_argument("--landing-dir", default=settings.landing_dir)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    folder = Path(args.landing_dir)
    name = f"labs_day_{args.day:03d}.csv"
    path = next((p for p in (folder / name, folder / "processed" / name) if p.exists()), None)
    if path is None:
        print(f"lab file {name} not found in {folder}", file=sys.stderr)
        return 1
    risk = lab_risk_from_csv(path)
    for pid, (points, tests) in sorted(risk.items()):
        print(f"{pid}  points={points}  {', '.join(tests) or '-'}")
    if args.dry_run:
        return 0

    import psycopg2

    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        dbname=settings.postgres_db,
    )
    with conn, conn.cursor() as cur:
        for pid, (points, tests) in risk.items():
            cur.execute(
                "INSERT INTO patient_lab_risk "
                "(patient_id, lab_risk_points, abnormal_tests, as_of_sim_day) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT (patient_id, as_of_sim_day) DO UPDATE SET "
                "lab_risk_points = EXCLUDED.lab_risk_points, "
                "abnormal_tests = EXCLUDED.abnormal_tests, "
                "computed_at = now()",
                (pid, points, tests, args.day),
            )
    conn.close()
    print(f"upserted {len(risk)} rows into patient_lab_risk (as_of_sim_day={args.day})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
