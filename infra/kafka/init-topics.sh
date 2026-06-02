#!/bin/bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"

create_topic () {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 \
    --config "cleanup.policy=delete" \
    --config "retention.ms=$retention_ms" \
    --config "min.insync.replicas=1" \
    >/dev/null
}

# 7 days retention for replay/debug; 3 partitions to allow per-symbol parallelism later.
create_topic "crypto_trades" 3 $((7 * 24 * 60 * 60 * 1000))

# DLQ is typically lower throughput; keep longer for audits.
create_topic "dead_letter_queue" 1 $((30 * 24 * 60 * 60 * 1000))

echo "Kafka topics ready: crypto_trades, dead_letter_queue"
