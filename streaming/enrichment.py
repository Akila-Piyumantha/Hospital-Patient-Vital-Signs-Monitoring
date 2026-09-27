"""Enrichment of validated readings (tasks B4 + B8).

* stream-static join with ``patients`` - drives the ``unknown_patient`` rule;
* derived vitals: MAP and pulse pressure;
* per-reading NEWS sub-scores and ``news_score`` (same thresholds as the batch layer,
  built from ``common.scoring.BANDS``);
* stream-static join with ``patient_lab_risk`` (lab feedback loop, B8): yesterday's lab
  points are added to the vitals score, so a lab-abnormal patient moves up a risk tier.

The two static sides are JDBC DataFrames that are **not cached**: Spark re-plans the static
side for every micro-batch, so a new ``patient_lab_risk`` row written by the Airflow DAG is
picked up by the very next batch (<= trigger interval) without restarting the job.
"""

from __future__ import annotations

from typing import Any

from common import scoring
from streaming.validation import parse_kafka, rejection_reason_col

# Latest lab risk per patient; the table keeps one row per lab day (history for the report).
LAB_RISK_QUERY = (
    "SELECT DISTINCT ON (patient_id) patient_id, lab_risk_points, "
    "as_of_sim_day AS lab_as_of_sim_day "
    "FROM patient_lab_risk ORDER BY patient_id, as_of_sim_day DESC"
)


def jdbc_reader(spark: Any, jdbc_url: str, user: str, password: str) -> Any:
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
    )


def static_patients(reader: Any) -> Any:
    return reader.option("query", "SELECT patient_id FROM patients").load()


def static_lab_risk(reader: Any) -> Any:
    return reader.option("query", LAB_RISK_QUERY).load()


def add_scores(df: Any) -> Any:
    """Derived vitals, NEWS sub-scores, lab points, total score and risk tier."""
    from pyspark.sql import functions as F

    df = df.withColumn(
        "map",
        F.round(F.col("diastolic_bp") + (F.col("systolic_bp") - F.col("diastolic_bp")) / 3, 1),
    ).withColumn("pulse_pressure", F.col("systolic_bp") - F.col("diastolic_bp"))
    for vital in scoring.SCORED_VITALS:
        df = df.withColumn(f"{vital}_score", scoring.vital_score_col(vital))
    sub_scores = [F.col(f"{v}_score") for v in scoring.SCORED_VITALS]
    df = (
        df.withColumn("news_score", sum(sub_scores[1:], sub_scores[0]))
        .withColumn("max_vital_score", F.greatest(*sub_scores))
        .withColumn("lab_risk_points", F.coalesce(F.col("lab_risk_points"), F.lit(0)))
        .withColumn("total_score", F.col("news_score") + F.col("lab_risk_points"))
    )
    return df.withColumn(
        "risk_tier", scoring.risk_tier_col(F.col("total_score"), F.col("max_vital_score"))
    )


def build_readings(kafka_df: Any, patients_df: Any, lab_risk_df: Any | None) -> Any:
    """Kafka stream -> parsed, validated (``rejection_reason``) and enriched readings."""
    from pyspark.sql import functions as F

    known = patients_df.select("patient_id").withColumn("_known", F.lit(True))
    df = parse_kafka(kafka_df).join(F.broadcast(known), "patient_id", "left")
    df = df.withColumn(
        "rejection_reason", rejection_reason_col(F.coalesce(F.col("_known"), F.lit(False)))
    ).drop("_known")
    if lab_risk_df is not None:
        df = df.join(F.broadcast(lab_risk_df), "patient_id", "left")
    else:
        df = df.withColumn("lab_risk_points", F.lit(None).cast("int")).withColumn(
            "lab_as_of_sim_day", F.lit(None).cast("int")
        )
    return add_scores(df)
