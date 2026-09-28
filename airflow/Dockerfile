# Airflow (LocalExecutor) + JDK + PySpark for the batch layer (Member C, task C2).
# Build context = repository root (the image carries `common`, `batch` and `sql`; Compose also
# bind-mounts them for development, so code changes need no rebuild).
#
# Airflow 2.10 on Python 3.11 (the shared `common` package needs 3.11). PySpark from pip bundles
# Spark 3.5.3 - the same version as the speed layer, so both layers score with one engine.
# The batch job only reads Parquet and talks to Postgres through psycopg2: no extra jars needed.
FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
# no JAVA_HOME: pyspark finds `java` on PATH (the JDK folder name differs between amd64 and arm64)
ENV TZ=UTC \
    PYTHONPATH=/opt/airflow

USER airflow
# optional PyPI mirror for slow links: docker compose build --build-arg PIP_INDEX_URL=<mirror>/simple
ARG PIP_INDEX_URL=https://pypi.org/simple
COPY airflow/requirements.txt /tmp/requirements.txt
# pin Airflow itself so pip never up/downgrades it while resolving the extra packages
RUN pip install --no-cache-dir --default-timeout=300 --retries 10 \
    "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

COPY --chown=airflow:root common /opt/airflow/common
COPY --chown=airflow:root batch /opt/airflow/batch
COPY --chown=airflow:root sql /opt/airflow/sql
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
