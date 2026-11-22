#!/usr/bin/env bash
# Creates sample data for local development and integration tests.
# Runs in a separate dev-only container; StreamSage itself never writes to Kafka.
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:19092}"
BIN="/opt/kafka/bin"

create_topic() {
  local name="$1" partitions="$2"
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1
}

create_topic orders.created 3
create_topic payments.processed 2
create_topic inventory.updated 1

# Joining a consumer group makes Kafka create the internal __consumer_offsets
# topic, so internal-topic filtering can be exercised locally.
"$BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
  --topic orders.created --group streamsage-sample-consumer \
  --timeout-ms 5000 > /dev/null 2>&1 || true

echo "Topics:"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list
