#!/bin/sh
# Creates the pipeline topics (idempotent). Runs once as the `kafka-init` Compose service.
#
# vitals.raw  3 partitions, key = patient_id
#             -> all readings of one patient go to one partition, preserving per-patient
#                order (needed for trend/slope maths). 3 partitions let up to 3 Spark
#                tasks read in parallel; more than 3 would add nothing with 20 patients
#                on one laptop-sized broker.
#             retention 24 h / 256 MB: the Parquet lake (batch layer) is the long-term
#                master dataset, Kafka only has to cover speed-layer restarts/replay.
# vitals.dlq  1 partition, retention 7 days: rejected records kept for inspection.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
VITALS_TOPIC="${VITALS_TOPIC:-vitals.raw}"
DLQ_TOPIC="${DLQ_TOPIC:-vitals.dlq}"
KT=/opt/kafka/bin/kafka-topics.sh

$KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$VITALS_TOPIC" --partitions "${VITALS_PARTITIONS:-3}" --replication-factor 1 \
    --config retention.ms=86400000 --config retention.bytes=268435456

$KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$DLQ_TOPIC" --partitions 1 --replication-factor 1 \
    --config retention.ms=604800000

echo "topics ready:"
$KT --bootstrap-server "$BOOTSTRAP" --describe --topic "$VITALS_TOPIC"
$KT --bootstrap-server "$BOOTSTRAP" --describe --topic "$DLQ_TOPIC"
