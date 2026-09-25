"""Soak-run monitor: samples the running stack and writes evidence for the report.

    python scripts/soak_monitor.py --minutes 30 --interval 30

Every ``--interval`` seconds it records (to ``data/soak/soak.csv``): current simulated day,
Kafka throughput, Prometheus targets up, firing alerts, container memory, restart count and
landed lab files. At the end it writes ``data/soak/summary.json`` (min/avg/max throughput,
peak memory, restarts, simulated days covered, alerts seen) - the numbers quoted in the
"Results" chapter. Standard library only.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "soak"
PROM = "http://localhost:9090"


def prom(expr: str) -> list[dict]:
    url = f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)["data"]["result"]


def scalar(expr: str, default: float = float("nan")) -> float:
    try:
        result = prom(expr)
        return float(result[0]["value"][1]) if result else default
    except Exception:
        return default


def docker(*args: str) -> str:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=60).stdout


def memory_mib() -> float:
    total = 0.0
    for line in docker("stats", "--no-stream", "--format", "{{.MemUsage}}").splitlines():
        used = line.split("/")[0].strip()
        for suffix, factor in (("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024), ("B", 1 / 1048576)):
            if used.endswith(suffix):
                total += float(used[: -len(suffix)]) * factor
                break
    return total


def restart_count() -> int:
    ids = docker("compose", "ps", "-q").split()
    if not ids:
        return 0
    out = docker("inspect", "--format", "{{.RestartCount}}", *ids)
    return sum(int(x) for x in out.split() if x.isdigit())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=30)
    parser.add_argument("--interval", type=float, default=30)
    args = parser.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    start = time.time()
    end = start + args.minutes * 60
    rows: list[dict] = []
    alerts_seen: set[str] = set()
    fields = [
        "elapsed_s",
        "sim_day",
        "msgs_per_s",
        "targets_up",
        "firing_alerts",
        "mem_mib",
        "restarts",
        "lab_files",
    ]

    with (OUT / "soak.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        while time.time() < end:
            firing = [r["metric"]["alertname"] for r in prom('ALERTS{alertstate="firing"}')]
            alerts_seen.update(firing)
            row = {
                "elapsed_s": round(time.time() - start),
                "sim_day": scalar("max(simulator_sim_day)"),
                "msgs_per_s": round(
                    scalar(
                        'sum(rate(kafka_topic_partition_current_offset{topic="vitals.raw"}[1m]))'
                    ),
                    2,
                ),
                "targets_up": scalar("sum(up)"),
                "firing_alerts": len(firing),
                "mem_mib": round(memory_mib()),
                "restarts": restart_count(),
                "lab_files": len(list((ROOT / "data" / "landing").rglob("labs_day_*.csv"))),
            }
            rows.append(row)
            writer.writerow(row)
            handle.flush()
            time.sleep(max(0.0, args.interval - 2))

    rates = [
        r["msgs_per_s"] for r in rows if r["msgs_per_s"] == r["msgs_per_s"] and r["msgs_per_s"] > 0
    ]
    days = [r["sim_day"] for r in rows if r["sim_day"] == r["sim_day"]]
    summary = {
        "duration_min": round((time.time() - start) / 60, 1),
        "samples": len(rows),
        "sim_days_covered": (max(days) - min(days) + 1) if days else None,
        "last_sim_day": max(days) if days else None,
        "msgs_per_s": {
            "min": min(rates),
            "avg": round(statistics.mean(rates), 2),
            "max": max(rates),
        }
        if rates
        else None,
        "peak_memory_mib": max(r["mem_mib"] for r in rows),
        "container_restarts": rows[-1]["restarts"] - rows[0]["restarts"],
        "lab_files_landed": rows[-1]["lab_files"],
        "alerts_seen_firing": sorted(alerts_seen),
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
