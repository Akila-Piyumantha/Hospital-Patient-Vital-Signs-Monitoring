# Spark Structured Streaming job (Member B). Build context = repository root (imports `common`).
#
# python:3.11 + JRE instead of the apache/spark image: that image ships Python 3.10, while the
# shared `common` package needs 3.11. PySpark from pip bundles Spark 3.5.3 itself; the Kafka
# connector and the Postgres JDBC driver are added to its jars/ folder, versions pinned to
# the ones Spark 3.5.3 was built against (spark-sql-kafka-0-10 -> kafka-clients 3.4.1).
# bookworm pinned: newer Debian (trixie) ships only Java 21; Spark 3.5 is tested on 17.
FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL_VERSION=2.11.1
ARG POSTGRES_JDBC_VERSION=42.7.4

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=UTC \
    PYSPARK_PYTHON=python

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY streaming/requirements.txt streaming/requirements.txt
# long timeout + retries: pyspark is ~317 MB and slow links time out with pip defaults
# optional PyPI mirror for slow links: docker compose build --build-arg PIP_INDEX_URL=<mirror>/simple
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install --default-timeout=300 --retries 10 -r streaming/requirements.txt

RUN JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && M=https://repo1.maven.org/maven2 \
    && curl -fsSL -o "$JARS/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
        "$M/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    && curl -fsSL -o "$JARS/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
        "$M/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    && curl -fsSL -o "$JARS/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
        "$M/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
    && curl -fsSL -o "$JARS/commons-pool2-${COMMONS_POOL_VERSION}.jar" \
        "$M/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar" \
    && curl -fsSL -o "$JARS/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
        "$M/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

COPY common ./common
COPY streaming ./streaming
COPY sql ./sql

EXPOSE 8003 4040
CMD ["python", "-m", "streaming.stream_job"]
