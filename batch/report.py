"""Daily consolidated risk report (task C5).

For file day *N* (``labs_day_N.csv`` = labs collected on day N-1) the report answers, per
patient: *how do yesterday's lab results change the risk picture going forward?*

* vitals of day N-1 from the batch recompute (``batch_vitals_daily``); when the lake had no data
  for that day, the speed layer's current ``patient_status`` is used instead (``vitals_source``);
* ``risk_before_labs`` = tier of *end-of-day NEWS + lab points in force during day N-1*;
* ``risk_after_labs``  = tier of *the same NEWS + lab points from this file* - exactly what the
  speed layer applies from day N on (it joins the newest ``patient_lab_risk`` row);
* ranked by risk after labs, then total score, then NEWS.

Outputs: ``patient_risk_report`` rows, ``data/reports/risk_report_day_NNN.html`` (self-contained,
sparklines as inline SVG) and ``risk_report_day_NNN.csv``. ``build_report`` is pure and tested.
"""

from __future__ import annotations

import csv
import html
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common import scoring


@dataclass(frozen=True)
class LabLine:
    test_type: str
    result_value: float
    abnormal_flag: str | None


@dataclass
class ReportRow:
    sim_day: int
    patient_id: str
    rank: int
    vitals_sim_day: int
    vitals_source: str
    vitals_summary: str
    lab_summary: str
    news_score: int
    max_vital_score: int
    trend: str | None
    lab_points_before: int
    lab_points_after: int
    total_before: int
    total_after: int
    risk_before_labs: str
    risk_after_labs: str
    tier_change: str
    abnormal_tests: list[str] = field(default_factory=list)
    alerts_opened: int = 0
    # presentation only (not stored in the table)
    name: str = ""
    bed: str = ""
    hr_series: list = field(default_factory=list)
    spo2_series: list = field(default_factory=list)


TABLE_COLUMNS = (
    "sim_day",
    "patient_id",
    "rank",
    "vitals_sim_day",
    "vitals_source",
    "vitals_summary",
    "lab_summary",
    "news_score",
    "max_vital_score",
    "trend",
    "lab_points_before",
    "lab_points_after",
    "total_before",
    "total_after",
    "risk_before_labs",
    "risk_after_labs",
    "tier_change",
    "abnormal_tests",
    "alerts_opened",
)


# ------------------------------------------------------------------------------ summaries
def _fmt(value: Any, digits: int = 0) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def vitals_summary(v: Mapping[str, Any] | None, source: str) -> str:
    if source == "none" or not v:
        return "no vitals recorded"
    if source == "speed":
        return (
            f"HR {_fmt(v.get('heart_rate'))} · SpO2 {_fmt(v.get('spo2'))} · "
            f"SBP {_fmt(v.get('systolic_bp'))} · T {_fmt(v.get('temperature'), 1)} "
            f"(latest reading, speed layer)"
        )
    parts = [
        f"HR {_fmt(v['avg_heart_rate'])} ({_fmt(v['min_heart_rate'])}-{_fmt(v['max_heart_rate'])})",
        f"SpO2 {_fmt(v['avg_spo2'])} ({_fmt(v['min_spo2'])}-{_fmt(v['max_spo2'])})",
        f"SBP {_fmt(v['avg_systolic_bp'])} "
        f"({_fmt(v['min_systolic_bp'])}-{_fmt(v['max_systolic_bp'])})",
        f"T {_fmt(v['avg_temperature'], 1)}",
        f"peak NEWS {_fmt(v.get('peak_news_score'))}",
    ]
    trend = v.get("trend") or "STABLE"
    if trend != "STABLE":
        moves = [
            f"{label} {change:+.0f}"
            for label, change in (
                ("HR", v.get("hr_change")),
                ("SpO2", v.get("spo2_change")),
                ("SBP", v.get("sbp_change")),
            )
            if change is not None and abs(change) >= 1
        ]
        parts.append(f"{trend} ({', '.join(moves)})" if moves else trend)
    return " · ".join(parts)


def lab_summary(lines: list[LabLine] | None) -> str:
    if not lines:
        return "no labs in this file"
    abnormal = [
        f"{line.test_type} {line.result_value:g} {'↑' if line.abnormal_flag == 'HIGH' else '↓'}"
        for line in sorted(lines, key=lambda x: x.test_type)
        if line.abnormal_flag
    ]
    return ", ".join(abnormal) if abnormal else f"{len(lines)} tests, all within range"


def tier_change(before: str, after: str) -> str:
    delta = scoring.TIER_ORDER[after] - scoring.TIER_ORDER[before]
    return "UP" if delta > 0 else "DOWN" if delta < 0 else "SAME"


# ---------------------------------------------------------------------------------- build
def build_report(
    day: int,
    patients: list[Mapping[str, Any]],
    batch_vitals: Mapping[str, Mapping[str, Any]],
    speed_status: Mapping[str, Mapping[str, Any]],
    lab_before: Mapping[str, int],
    lab_after: Mapping[str, tuple[int, list[str]]],
    lab_lines: Mapping[str, list[LabLine]],
    alerts_opened: Mapping[str, int] | None = None,
) -> list[ReportRow]:
    """Ranked report rows for file day ``day`` (pure; inputs are plain mappings).

    ``lab_before``: patient -> points in force during day N-1 (newest ``as_of_sim_day < N``).
    ``lab_after``: patient -> (points, abnormal tests) after this file (newest ``<= N``).
    """
    alerts_opened = alerts_opened or {}
    rows: list[ReportRow] = []
    for p in patients:
        pid = p["patient_id"]
        if pid in batch_vitals:
            v, source = batch_vitals[pid], "batch"
            news, max_vital = v["end_news_score"], v["end_max_vital_score"]
            trend = v.get("trend")
        elif pid in speed_status and speed_status[pid].get("news_score") is not None:
            v, source = speed_status[pid], "speed"
            news, max_vital = v["news_score"], v.get("max_vital_score") or 0
            trend = None
        else:
            v, source, news, max_vital, trend = None, "none", 0, 0, None
        before = lab_before.get(pid, 0)
        after, abnormal = lab_after.get(pid, (before, []))
        tier_b = scoring.risk_tier(news + before, max_vital)
        tier_a = scoring.risk_tier(news + after, max_vital)
        rows.append(
            ReportRow(
                sim_day=day,
                patient_id=pid,
                rank=0,
                vitals_sim_day=day - 1,
                vitals_source=source,
                vitals_summary=vitals_summary(v, source),
                lab_summary=lab_summary(lab_lines.get(pid)),
                news_score=news,
                max_vital_score=max_vital,
                trend=trend,
                lab_points_before=before,
                lab_points_after=after,
                total_before=news + before,
                total_after=news + after,
                risk_before_labs=tier_b,
                risk_after_labs=tier_a,
                tier_change=tier_change(tier_b, tier_a),
                abnormal_tests=list(abnormal),
                alerts_opened=alerts_opened.get(pid, 0),
                name=p.get("name", ""),
                bed=p.get("bed", ""),
                hr_series=list((v or {}).get("hr_series") or []) if source == "batch" else [],
                spo2_series=list((v or {}).get("spo2_series") or []) if source == "batch" else [],
            )
        )
    rows.sort(
        key=lambda r: (
            -scoring.TIER_ORDER[r.risk_after_labs],
            -r.total_after,
            -r.news_score,
            r.patient_id,
        )
    )
    for i, r in enumerate(rows, start=1):
        r.rank = i
    return rows


# ---------------------------------------------------------------------------- persistence
def fetch_inputs(cur: Any, day: int, day_bounds: tuple[datetime, datetime] | None) -> dict:
    """Everything ``build_report`` needs for file day ``day``, read from Postgres."""
    cur.execute("SELECT patient_id, name, bed FROM patients ORDER BY patient_id")
    patients = [{"patient_id": r[0], "name": r[1], "bed": r[2]} for r in cur.fetchall()]

    cur.execute("SELECT * FROM batch_vitals_daily WHERE sim_day = %s", (day - 1,))
    cols = [d[0] for d in cur.description]
    batch_vitals = {}
    for r in cur.fetchall():
        row = dict(zip(cols, r, strict=True))
        batch_vitals[row["patient_id"]] = row

    cur.execute(
        "SELECT patient_id, heart_rate, spo2, systolic_bp, temperature, news_score, "
        "max_vital_score FROM patient_status"
    )
    cols = [d[0] for d in cur.description]
    speed_status = {r[0]: dict(zip(cols, r, strict=True)) for r in cur.fetchall()}

    cur.execute(
        "SELECT DISTINCT ON (patient_id) patient_id, lab_risk_points FROM patient_lab_risk "
        "WHERE as_of_sim_day < %s ORDER BY patient_id, as_of_sim_day DESC",
        (day,),
    )
    lab_before = {r[0]: r[1] for r in cur.fetchall()}
    cur.execute(
        "SELECT DISTINCT ON (patient_id) patient_id, lab_risk_points, abnormal_tests "
        "FROM patient_lab_risk WHERE as_of_sim_day <= %s ORDER BY patient_id, as_of_sim_day DESC",
        (day,),
    )
    lab_after = {r[0]: (r[1], list(r[2] or [])) for r in cur.fetchall()}

    cur.execute(
        "SELECT patient_id, test_type, result_value, abnormal_flag FROM lab_results "
        "WHERE sim_day = %s",
        (day,),
    )
    lab_lines: dict[str, list[LabLine]] = {}
    for pid, test, value, flag in cur.fetchall():
        lab_lines.setdefault(pid, []).append(LabLine(test, value, flag))

    alerts: dict[str, int] = {}
    if day_bounds is not None:
        cur.execute(
            "SELECT patient_id, count(*) FROM alerts WHERE opened_at >= %s AND opened_at < %s "
            "GROUP BY patient_id",
            day_bounds,
        )
        alerts = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute(
        "SELECT discrepancy_ratio FROM speed_batch_reconciliation WHERE sim_day = %s", (day - 1,)
    )
    recon = cur.fetchone()
    return {
        "patients": patients,
        "batch_vitals": batch_vitals,
        "speed_status": speed_status,
        "lab_before": lab_before,
        "lab_after": lab_after,
        "lab_lines": lab_lines,
        "alerts_opened": alerts,
        "discrepancy_ratio": recon[0] if recon else None,
    }


def write_report_table(cur: Any, day: int, rows: list[ReportRow]) -> int:
    from psycopg2.extras import execute_values

    cur.execute("DELETE FROM patient_risk_report WHERE sim_day = %s", (day,))
    values = [tuple(getattr(r, c) for c in TABLE_COLUMNS) for r in rows]
    if values:
        execute_values(
            cur,
            f"INSERT INTO patient_risk_report ({', '.join(TABLE_COLUMNS)}) VALUES %s",
            values,
        )
    return len(values)


def report_paths(reports_dir: Path, day: int) -> tuple[Path, Path]:
    stem = f"risk_report_day_{day:03d}"
    return reports_dir / f"{stem}.html", reports_dir / f"{stem}.csv"


def write_csv(path: Path, rows: list[ReportRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".csv.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([*TABLE_COLUMNS, "name", "bed"])
        for r in rows:
            d = asdict(r)
            d["abnormal_tests"] = ";".join(r.abnormal_tests)
            writer.writerow([d[c] for c in TABLE_COLUMNS] + [r.name, r.bed])
    tmp.replace(path)


# ------------------------------------------------------------------------------------ HTML
REPORT_CSS = """
  :root { --fg:#1d232a; --muted:#66707a; --line:#dde2e6; --bg:#fff;
          --up:#fff3e6; --down:#eef7ee; }
  body { font: 14px/1.45 system-ui, -apple-system, Segoe UI, sans-serif; color:var(--fg);
         background:var(--bg); margin: 24px; }
  h1 { font-size: 22px; margin: 0 0 4px; }
  .muted { color: var(--muted); font-size: 12px; }
  .cards { display:flex; gap:12px; flex-wrap:wrap; margin:16px 0; }
  .card { border:1px solid var(--line); border-radius:8px; padding:10px 14px; min-width:180px; }
  .card b { font-size: 18px; }
  .wrap { overflow-x:auto; }
  table { border-collapse: collapse; width: 100%; min-width: 1100px; }
  th, td { border-bottom: 1px solid var(--line); padding: 6px 8px; text-align: left;
           vertical-align: top; }
  th { font-size: 12px; color: var(--muted); font-weight: 600; }
  tr.chg-up { background: var(--up); } tr.chg-down { background: var(--down); }
  .tier { color:#fff; border-radius: 4px; padding: 1px 6px; font-size: 12px; font-weight:600; }
  .spark { vertical-align: middle; } td.hr { color:#c0392b; } td.spo2 { color:#2471a3; }
  .range { color: var(--muted); font-size: 11px; margin-left: 4px; }
"""

TIER_COLORS = {"LOW": "#2e7d32", "MEDIUM": "#b8860b", "HIGH": "#d35400", "CRITICAL": "#c0392b"}


def sparkline(values: list[float | None], width: int = 120, height: int = 26) -> str:
    """Inline SVG polyline; gaps (``None``) split the line."""
    points = [(i, v) for i, v in enumerate(values) if v is not None]
    if len(points) < 2:
        return '<span class="muted">-</span>'
    lo, hi = min(v for _, v in points), max(v for _, v in points)
    span = (hi - lo) or 1.0
    step = width / max(len(values) - 1, 1)
    segments: list[list[str]] = [[]]
    for i, v in enumerate(values):
        if v is None:
            segments.append([])
            continue
        y = height - 2 - (v - lo) / span * (height - 4)
        segments[-1].append(f"{i * step:.1f},{y:.1f}")
    lines = "".join(
        f'<polyline points="{" ".join(s)}" fill="none" stroke="currentColor" stroke-width="1.5"/>'
        for s in segments
        if len(s) > 1
    )
    return (
        f'<svg class="spark" width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="range {lo:g} to {hi:g}">{lines}</svg>'
        f'<span class="range">{lo:.0f}-{hi:.0f}</span>'
    )


def _badge(tier: str) -> str:
    return f'<span class="tier" style="background:{TIER_COLORS[tier]}">{tier}</span>'


def render_html(
    day: int, rows: list[ReportRow], discrepancy_ratio: float | None, generated_at: datetime
) -> str:
    esc = html.escape
    changed = [r for r in rows if r.tier_change != "SAME"]
    counts = {t: sum(1 for r in rows if r.risk_after_labs == t) for t in scoring.TIER_ORDER}
    body_rows = []
    for r in rows:
        body_rows.append(
            f"<tr class='chg-{r.tier_change.lower()}'>"
            f"<td>{r.rank}</td>"
            f"<td><b>{esc(r.patient_id)}</b><br><span class='muted'>{esc(r.name)} · "
            f"bed {esc(r.bed)}</span></td>"
            f"<td>{_badge(r.risk_before_labs)}</td>"
            f"<td>{_badge(r.risk_after_labs)} "
            f"{'▲' if r.tier_change == 'UP' else '▼' if r.tier_change == 'DOWN' else ''}</td>"
            f"<td>{r.news_score}</td>"
            f"<td>{r.lab_points_before} → <b>{r.lab_points_after}</b></td>"
            f"<td>{esc(r.trend or '-')}</td>"
            f"<td class='hr'>{sparkline(r.hr_series)}</td>"
            f"<td class='spo2'>{sparkline(r.spo2_series)}</td>"
            f"<td>{esc(r.vitals_summary)}</td>"
            f"<td>{esc(r.lab_summary)}</td>"
            f"<td>{r.alerts_opened}</td>"
            "</tr>"
        )
    changed_list = (
        "".join(
            f"<li><b>{esc(r.patient_id)}</b>: {r.risk_before_labs} → {r.risk_after_labs} "
            f"(lab points {r.lab_points_before} → {r.lab_points_after}: "
            f"{esc(', '.join(r.abnormal_tests) or 'labs normalised')})</li>"
            for r in changed
        )
        or "<li>No patient changed risk tier because of this lab file.</li>"
    )
    disc = (
        "not available (no lake data for the day)"
        if discrepancy_ratio is None
        else f"{discrepancy_ratio:.2%} of readings"
    )
    tier_counts = " · ".join(f"{t.title()} {n}" for t, n in counts.items())
    ups = sum(r.tier_change == "UP" for r in rows)
    downs = sum(r.tier_change == "DOWN" for r in rows)
    stamp = generated_at.strftime("%Y-%m-%d %H:%M:%S")
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Risk report - day {day}</title>
<style>{REPORT_CSS}</style></head><body>
<h1>Daily consolidated risk report - simulated day {day}</h1>
<div class="muted">Vitals of day {day - 1} (batch recompute from the Parquet lake) combined
with the lab file <code>labs_day_{day:03d}.csv</code> (labs collected on day {day - 1}).
Generated {stamp} UTC. Scores: simplified NEWS2 adaptation, not clinically validated.</div>
<div class="cards">
  <div class="card">Patients<br><b>{len(rows)}</b></div>
  <div class="card">Risk after labs<br><b>{tier_counts}</b></div>
  <div class="card">Tier changed by labs<br><b>{len(changed)}</b>
    ({ups} up, {downs} down)</div>
  <div class="card">Speed vs batch discrepancy<br><b>{disc}</b></div>
</div>
<h2>How yesterday's labs change the risk picture</h2>
<ul>{changed_list}</ul>
<h2>All patients, ranked by risk after labs</h2>
<div class="wrap"><table>
<thead><tr><th>#</th><th>Patient</th><th>Risk before labs</th><th>Risk after labs</th>
<th>End-of-day NEWS</th><th>Lab points</th><th>Trend</th><th>HR (day)</th><th>SpO2 (day)</th>
<th>Vitals summary</th><th>Lab flags</th><th>Alerts</th></tr></thead>
<tbody>{"".join(body_rows)}</tbody></table></div>
</body></html>
"""


def write_html(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".html.tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def now_utc() -> datetime:
    return datetime.now(UTC)
