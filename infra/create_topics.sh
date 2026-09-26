#!/bin/bash
# One-shot topic creation. Idempotent: safe to re-run on an existing cluster.
set -euo pipefail

KT=/opt/kafka/bin/kafka-topics.sh
BS="${KAFKA_BOOTSTRAP:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
RF=3

create() {
  local name=$1 partitions=$2
  shift 2
  local configs=()
  for c in "$@"; do configs+=(--config "$c"); done
  "$KT" --bootstrap-server "$BS" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor "$RF" \
    --config min.insync.replicas=2 "${configs[@]}"
}

THREE_DAYS_MS=259200000
ONE_DAY_MS=86400000

create parking.raw 6 "retention.ms=${THREE_DAYS_MS}"
create parking.dlq 1 "retention.ms=$((7 * ONE_DAY_MS))"
create parking.late 1 "retention.ms=$((7 * ONE_DAY_MS))"
create lot.metadata 1 cleanup.policy=compact
create lot.occupancy.5min 3 "retention.ms=$((7 * ONE_DAY_MS))"
create lot.alerts 1 "retention.ms=$((7 * ONE_DAY_MS))"
create state.changelog 6 cleanup.policy=compact min.cleanable.dirty.ratio=0.1 segment.ms=60000

echo "--- topic layout ---"
"$KT" --bootstrap-server "$BS" --describe
