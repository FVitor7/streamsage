#!/usr/bin/env bash
# Creates sample data for local development and integration tests.
# Runs in a separate dev-only container; StreamSage itself never writes to Kafka.
#
# Topics:
#   orders.created      3 partitions, 300 keyed messages
#   payments.processed  2 partitions, 50 keyed messages
#   inventory.updated   1 partition,  20 messages
#
# Consumer groups (committed offsets set explicitly, so lag is deterministic):
#   orders-service    orders.created, committed offset 40 on every partition
#                     -> lag = (messages in partition - 40) per partition
#   payments-service  payments.processed, fully caught up -> lag 0
#   inventory-service consumed continuously by the `kafka-consumer` service
#                     (an active group with a member)
#
# Data is seeded only once per broker volume; re-running is a no-op.
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:19092}"
BIN="/opt/kafka/bin"

create_topic() {
  local name="$1" partitions="$2"
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists --topic "$name" \
    --partitions "$partitions" --replication-factor 1
}

produce_keyed() {
  local topic="$1" count="$2"
  seq 1 "$count" | sed 's/.*/key-&:{"seq":&}/' |
    "$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
      --topic "$topic" \
      --property parse.key=true --property key.separator=: > /dev/null
}

# Usage: set_committed_offsets GROUP TOPIC RESET-OPTION...
set_committed_offsets() {
  local group="$1" topic="$2"
  shift 2
  "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --group "$group" --topic "$topic" --reset-offsets "$@" --execute > /dev/null
}

group_exists() {
  "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list |
    grep -qx "$1"
}

create_topic orders.created 3
create_topic payments.processed 2
create_topic inventory.updated 1

if group_exists orders-service; then
  echo "Sample data already seeded; skipping."
else
  produce_keyed orders.created 300
  produce_keyed payments.processed 50
  produce_keyed inventory.updated 20
  set_committed_offsets orders-service orders.created --to-offset 40
  set_committed_offsets payments-service payments.processed --to-latest
  echo "Seeded sample messages and consumer groups."
fi

echo "Topics:"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list
echo "Consumer groups:"
"$BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list
