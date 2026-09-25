"""Generates the provisioned Grafana dashboards (owner: Member A).

    python observability/grafana/build_dashboards.py

Writes ``dashboards/pipeline-health.json`` and ``dashboards/ward-live.json``. The JSON is
committed so ``docker compose up`` needs no build step; this script exists so panels stay
readable/reviewable as code instead of 1000-line JSON diffs.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
PG = {"type": "grafana-postgresql-datasource", "uid": "hospital-db"}

RISK_COLORS = {"LOW": "green", "MEDIUM": "yellow", "HIGH": "orange", "CRITICAL": "red"}


def thresholds(*steps: tuple[float | None, str]) -> dict:
    return {
        "mode": "absolute",
        "steps": [{"color": color, "value": value} for value, color in steps],
    }


class Dashboard:
    def __init__(self, uid: str, title: str, refresh: str, description: str) -> None:
        self.uid, self.title, self.refresh, self.description = uid, title, refresh, description
        self.panels: list[dict] = []
        self.templating: list[dict] = []
        self._id = 0

    def _panel(
        self,
        kind: str,
        title: str,
        pos: tuple[int, int, int, int],
        ds: dict,
        targets: list[dict],
        unit: str = "short",
        desc: str = "",
        th: dict | None = None,
        options: dict | None = None,
        overrides: list | None = None,
        extra_defaults: dict | None = None,
    ) -> None:
        self._id += 1
        x, y, w, h = pos
        defaults = {
            "unit": unit,
            "thresholds": th or thresholds((None, "green")),
            **(extra_defaults or {}),
        }
        self.panels.append(
            {
                "id": self._id,
                "type": kind,
                "title": title,
                "description": desc,
                "gridPos": {"x": x, "y": y, "w": w, "h": h},
                "datasource": ds,
                "targets": targets,
                "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
                "options": options or {},
            }
        )

    # -- Prometheus helpers ---------------------------------------------------------------
    @staticmethod
    def _prom(
        expr: str, legend: str = "", instant: bool = False, table: bool = False, ref: str = "A"
    ) -> dict:
        target = {
            "refId": ref,
            "datasource": PROM,
            "expr": expr,
            "legendFormat": legend,
            "instant": instant,
            "range": not instant,
        }
        if table:
            target["format"] = "table"
        return target

    def prom_stat(self, title, pos, expr, unit="short", th=None, desc="", decimals=None):
        defaults = {"decimals": decimals} if decimals is not None else None
        self._panel(
            "stat",
            title,
            pos,
            PROM,
            [self._prom(expr, instant=True)],
            unit,
            desc,
            th,
            {
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                "colorMode": "background",
                "graphMode": "area",
                "textMode": "value",
            },
            extra_defaults=defaults,
        )

    def prom_series(self, title, pos, queries, unit="short", desc="", stack=False):
        targets = [self._prom(e, legend, ref=chr(65 + i)) for i, (e, legend) in enumerate(queries)]
        self._panel(
            "timeseries",
            title,
            pos,
            PROM,
            targets,
            unit,
            desc,
            options={
                "legend": {"displayMode": "list", "placement": "bottom"},
                "tooltip": {"mode": "multi"},
            },
            extra_defaults={
                "custom": {
                    "lineWidth": 2,
                    "fillOpacity": 12 if stack else 0,
                    "stacking": {"mode": "normal" if stack else "none"},
                    "showPoints": "never",
                }
            },
        )

    def prom_table(self, title, pos, expr, desc=""):
        self._panel(
            "table",
            title,
            pos,
            PROM,
            [self._prom(expr, instant=True, table=True)],
            desc=desc,
            options={"showHeader": True},
        )

    # -- Postgres helpers -----------------------------------------------------------------
    @staticmethod
    def _sql(sql: str, fmt: str = "table") -> dict:
        return {
            "refId": "A",
            "datasource": PG,
            "format": fmt,
            "rawQuery": True,
            "editorMode": "code",
            "rawSql": sql,
        }

    def sql_stat(self, title, pos, sql, unit="short", th=None, desc=""):
        self._panel(
            "stat",
            title,
            pos,
            PG,
            [self._sql(sql)],
            unit,
            desc,
            th,
            {
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                "colorMode": "background",
                "graphMode": "none",
                "textMode": "value",
            },
        )

    def sql_table(self, title, pos, sql, desc="", risk_column: str | None = None):
        overrides = []
        if risk_column:
            overrides.append(
                {
                    "matcher": {"id": "byName", "options": risk_column},
                    "properties": [
                        {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                        {
                            "id": "mappings",
                            "value": [
                                {
                                    "type": "value",
                                    "options": {
                                        tier: {"color": color, "index": i}
                                        for i, (tier, color) in enumerate(RISK_COLORS.items())
                                    },
                                }
                            ],
                        },
                    ],
                }
            )
        self._panel(
            "table",
            title,
            pos,
            PG,
            [self._sql(sql)],
            desc=desc,
            options={"showHeader": True, "cellHeight": "sm"},
            overrides=overrides,
        )

    def sql_series(self, title, pos, sql, unit="short", desc="", th=None):
        self._panel(
            "timeseries",
            title,
            pos,
            PG,
            [self._sql(sql, "time_series")],
            unit,
            desc,
            th,
            options={"legend": {"displayMode": "list", "placement": "bottom"}},
            extra_defaults={"custom": {"lineWidth": 2, "showPoints": "never"}},
        )

    def text(self, title, pos, content):
        self._id += 1
        x, y, w, h = pos
        self.panels.append(
            {
                "id": self._id,
                "type": "text",
                "title": title,
                "gridPos": {"x": x, "y": y, "w": w, "h": h},
                "options": {"mode": "markdown", "content": content},
            }
        )

    def patient_variable(self) -> None:
        self.templating.append(
            {
                "name": "patient",
                "label": "Patient",
                "type": "query",
                "datasource": PG,
                "query": "SELECT patient_id FROM patients ORDER BY 1",
                "refresh": 1,
                "sort": 1,
                "current": {},
                "includeAll": False,
            }
        )

    def build(self) -> dict:
        return {
            "uid": self.uid,
            "title": self.title,
            "description": self.description,
            "tags": ["hospital", "pipeline"],
            "timezone": "browser",
            "schemaVersion": 39,
            "version": 1,
            "editable": False,
            "refresh": self.refresh,
            "time": {"from": "now-15m", "to": "now"},
            "templating": {"list": self.templating},
            "annotations": {"list": []},
            "panels": self.panels,
        }

    def write(self, filename: str) -> None:
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / filename).write_text(json.dumps(self.build(), indent=2) + "\n")


def pipeline_health() -> Dashboard:
    d = Dashboard(
        "pipeline-health",
        "Pipeline Health",
        "10s",
        "Ingestion, stream processing, batch and alerting health across the pipeline.",
    )
    red_above_0 = thresholds((None, "green"), (1, "red"))
    d.prom_stat(
        "Firing alerts",
        (0, 0, 4, 4),
        'count(ALERTS{alertstate="firing"}) or vector(0)',
        th=red_above_0,
    )
    d.prom_stat(
        "Scrape targets down",
        (4, 0, 4, 4),
        "count(up == 0) or vector(0)",
        th=thresholds((None, "green"), (1, "orange"), (3, "red")),
        desc="spark-streaming and api stay down until Members B/C add their services.",
    )
    d.prom_stat(
        "Readings / s into Kafka",
        (8, 0, 4, 4),
        'sum(rate(kafka_topic_partition_current_offset{topic="vitals.raw"}[1m]))',
        unit="ops",
        decimals=1,
        desc="Expected ~10/s with 20 patients at 2 s interval.",
        th=thresholds((None, "red"), (1, "green")),
    )
    d.prom_stat(
        "Seconds since last reading",
        (12, 0, 4, 4),
        "time() - max(simulator_last_emit_timestamp_seconds)",
        unit="s",
        decimals=0,
        th=thresholds((None, "green"), (30, "orange"), (60, "red")),
    )
    d.prom_stat(
        "DLQ share of readings",
        (16, 0, 4, 4),
        "sum(rate(vitals_dlq_total[2m])) / (sum(rate(vitals_valid_total[2m])) "
        "+ sum(rate(vitals_dlq_total[2m])))",
        unit="percentunit",
        decimals=1,
        th=thresholds((None, "green"), (0.05, "red")),
        desc="Alert DlqRateHigh fires above 5 %.",
    )
    d.prom_stat(
        "Simulated day",
        (20, 0, 4, 4),
        "max(simulator_sim_day)",
        desc="1 simulated day = 5 real minutes (SIM_DAY_SECONDS).",
    )

    d.text(
        "Layer legend",
        (0, 4, 24, 2),
        "**Ingestion** (simulators, Kafka) → **Processing** (Spark speed layer, Airflow batch "
        "layer) → **Storage/Serving** (Postgres, API). Rows below follow that order.",
    )

    d.prom_series(
        "Kafka: messages/s per partition (vitals.raw)",
        (0, 6, 12, 8),
        [
            (
                "sum by (partition)(rate(kafka_topic_partition_current_offset"
                '{topic="vitals.raw"}[1m]))',
                "partition {{partition}}",
            )
        ],
        unit="ops",
        desc="Keyed by patient_id: load is spread across the 3 partitions.",
        stack=True,
    )
    d.prom_series(
        "Producer: acknowledged vs failed",
        (12, 6, 12, 8),
        [
            ("sum(rate(vitals_produced_total[1m]))", "acknowledged/s"),
            ("sum(rate(vitals_produce_errors_total[1m]))", "errors/s"),
        ],
        unit="ops",
    )
    d.prom_series(
        "Injected faults by type",
        (0, 14, 12, 8),
        [("sum by (type)(rate(vitals_faults_injected_total[1m]))", "{{type}}")],
        unit="ops",
        desc="Deliberate data damage from FAULT_PROFILE.",
        stack=True,
    )
    d.prom_series(
        "Consumer lag (vitals.raw)",
        (12, 14, 12, 8),
        [
            (
                'sum by (consumergroup)(kafka_consumergroup_lag{topic="vitals.raw"})',
                "{{consumergroup}}",
            )
        ],
        desc="Needs a fixed kafka.group.id in the Spark job.",
    )

    d.prom_series(
        "Spark: input rate", (0, 22, 8, 8), [("spark_input_rows_per_sec", "rows/s")], unit="ops"
    )
    d.prom_series(
        "Spark: micro-batch duration",
        (8, 22, 8, 8),
        [("spark_batch_duration_seconds", "batch")],
        unit="s",
    )
    d.prom_series(
        "Valid vs rejected (DLQ) readings",
        (16, 22, 8, 8),
        [
            ("sum(rate(vitals_valid_total[1m]))", "valid/s"),
            ("sum(rate(vitals_dlq_total[1m]))", "dlq/s"),
        ],
        unit="ops",
    )

    d.prom_series(
        "Lab files: dropped vs ingested",
        (0, 30, 8, 8),
        [
            ("lab_files_dropped_total", "dropped by lab-generator"),
            ("lab_files_ingested_total", "ingested by Airflow"),
        ],
        desc="A widening gap means the batch layer is not consuming the daily file.",
    )
    d.prom_series(
        "Seconds since last DAG success",
        (8, 30, 8, 8),
        [("time() - airflow_dag_last_success_timestamp", "{{dag_id}}")],
        unit="s",
        desc="Alert DagFailed above 660 s (2 missed simulated days).",
    )
    d.prom_series(
        "Speed vs batch discrepancy",
        (16, 30, 8, 8),
        [("speed_batch_discrepancy_ratio", "discrepancy")],
        unit="percentunit",
        desc="Lambda consistency: batch recompute vs streaming aggregates.",
    )

    d.prom_series(
        "Alerts opened (by severity)",
        (0, 38, 8, 8),
        [("sum by (severity)(increase(alerts_opened_total[5m]))", "{{severity}}")],
        desc="Patient alerts raised by the streaming engine.",
        stack=True,
    )
    d.prom_series(
        "API latency p95",
        (8, 38, 8, 8),
        [
            (
                "histogram_quantile(0.95, sum by (le)"
                "(rate(api_request_duration_seconds_bucket[1m])))",
                "p95",
            )
        ],
        unit="s",
    )
    d.prom_series(
        "Simulator: active episodes / late events held",
        (16, 38, 8, 8),
        [
            ("simulator_active_episodes", "patients in episode"),
            ("simulator_late_events_pending", "late events pending"),
        ],
    )

    d.prom_table(
        "Scrape targets",
        (0, 46, 12, 8),
        "up",
        desc="1 = healthy, 0 = down. Health-check view of every component.",
    )
    d.prom_table("Firing alerts", (12, 46, 12, 8), 'ALERTS{alertstate="firing"}')
    return d


def ward_live() -> Dashboard:
    d = Dashboard(
        "ward-live",
        "Ward Live Monitoring",
        "5s",
        "Near-real-time patient risk, alerts and vitals (speed layer) plus the latest "
        "daily risk report (batch layer). Reads Postgres tables from contract 4.4.",
    )
    d.patient_variable()
    for i, (tier, color) in enumerate(RISK_COLORS.items()):
        d.sql_stat(
            f"{tier.title()} risk",
            (i * 4, 0, 4, 4),
            f"SELECT count(*) FROM patient_status WHERE risk_tier = '{tier}'",
            th=thresholds((None, "green" if tier == "LOW" else "text"), (1, color)),
        )
    d.sql_stat(
        "Open alerts",
        (16, 0, 4, 4),
        "SELECT count(*) FROM alerts WHERE resolved_at IS NULL",
        th=thresholds((None, "green"), (1, "orange"), (5, "red")),
    )
    d.sql_stat(
        "Data age (s)",
        (20, 0, 4, 4),
        "SELECT round(EXTRACT(EPOCH FROM now() - max(last_reading_at))) FROM patient_status",
        unit="s",
        th=thresholds((None, "green"), (15, "orange"), (60, "red")),
        desc="Freshness of the newest reading seen by the speed layer.",
    )

    d.sql_table(
        "Patients by risk",
        (0, 4, 16, 10),
        "SELECT s.patient_id, p.bed, s.risk_tier, s.news_score, s.lab_risk_points, "
        "s.trend_flag, s.heart_rate, s.spo2, s.systolic_bp, s.diastolic_bp, s.temperature, "
        "s.last_reading_at FROM patient_status s JOIN patients p USING (patient_id) "
        "ORDER BY CASE s.risk_tier WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 "
        "WHEN 'MEDIUM' THEN 3 ELSE 4 END, s.news_score DESC, s.patient_id",
        desc="Current status per patient (patient_status joined with the patients dimension).",
        risk_column="risk_tier",
    )
    d.sql_table(
        "Open alerts",
        (16, 4, 8, 10),
        "SELECT opened_at, patient_id, severity, reason_code, value, threshold FROM alerts "
        "WHERE resolved_at IS NULL ORDER BY opened_at DESC LIMIT 50",
    )

    d.sql_series(
        "Heart rate - $patient",
        (0, 14, 12, 7),
        'SELECT window_end AS time, avg_heart_rate AS "avg heart rate", '
        'max_heart_rate AS "max heart rate" FROM vitals_window '
        "WHERE patient_id = '$patient' AND $__timeFilter(window_end) ORDER BY 1",
        unit="none",
    )
    d.sql_series(
        "SpO2 - $patient",
        (12, 14, 12, 7),
        'SELECT window_end AS time, avg_spo2 AS "avg SpO2", min_spo2 AS "min SpO2" '
        "FROM vitals_window WHERE patient_id = '$patient' "
        "AND $__timeFilter(window_end) ORDER BY 1",
        unit="percent",
    )
    d.sql_series(
        "Systolic BP - $patient",
        (0, 21, 12, 7),
        'SELECT window_end AS time, avg_systolic_bp AS "avg systolic", '
        'min_systolic_bp AS "min systolic" FROM vitals_window '
        "WHERE patient_id = '$patient' AND $__timeFilter(window_end) ORDER BY 1",
    )
    d.sql_series(
        "Temperature - $patient",
        (12, 21, 12, 7),
        'SELECT window_end AS time, avg_temperature AS "avg temperature" '
        "FROM vitals_window WHERE patient_id = '$patient' "
        "AND $__timeFilter(window_end) ORDER BY 1",
        unit="celsius",
    )

    d.sql_series(
        "Alerts raised per minute",
        (0, 28, 12, 7),
        "SELECT $__timeGroupAlias(opened_at, 1m), count(*) AS alerts FROM alerts "
        "WHERE $__timeFilter(opened_at) GROUP BY 1 ORDER BY 1",
    )
    d.sql_table(
        "Daily risk report - latest simulated day",
        (12, 28, 12, 7),
        "SELECT rank, patient_id, risk_before_labs, risk_after_labs, lab_summary, "
        "vitals_summary FROM patient_risk_report WHERE sim_day = "
        "(SELECT max(sim_day) FROM patient_risk_report) ORDER BY rank",
        desc="Batch layer output: how yesterday's lab results changed each patient's risk.",
        risk_column="risk_after_labs",
    )
    return d


if __name__ == "__main__":
    pipeline_health().write("pipeline-health.json")
    ward_live().write("ward-live.json")
    print(f"wrote dashboards to {OUT}")
