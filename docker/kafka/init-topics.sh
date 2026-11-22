#!/usr/bin/env bash
# Creates sample data for local development and integration tests.
# Runs in a separate dev-only container; StreamSage itself never writes to Kafka.
#
# Topics:
#   orders.created      3 partitions, 300 keyed messages
#   payments.processed  2 partitions, 50 keyed messages
#   inventory.updated   1 partition,  20 messages
#   customers.updated   1 partition,  3 JSON messages with fake personal data
#                       and an Authorization header (exercises sanitization)
#   orders.created.DLQ  1 partition,  2 failed orders with error headers
#                       (a DLQ only when STREAMSAGE_DLQ_TOPICS matches it)
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

topic_is_empty() {
  local end
  end="$("$BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --topic "$1" | awk -F: '{sum += $3} END {print sum + 0}')"
  [[ "$end" == "0" ]]
}

group_exists() {
  "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list |
    grep -qx "$1"
}

# Each Kafka CLI call starts a JVM (several seconds), so create in parallel.
create_topic orders.created 3 &
create_topic payments.processed 2 &
create_topic inventory.updated 1 &
create_topic customers.updated 1 &
create_topic orders.created.DLQ 1 &
wait

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

# Fake data only. Credential-like values are random per seeding, so no
# secret-looking literal lives in the repository. Headers are "key:value"
# pairs separated by commas; '|' separates headers, key and value.
if topic_is_empty customers.updated; then
  fake() { echo "fake-$1-$RANDOM$RANDOM"; }
  printf '%s\n' \
    "Authorization:Bearer $(fake auth),trace-id:abc-001|cust-1|{\"id\":1,\"name\":\"Ana\",\"email\":\"ana@example.com\",\"password\":\"$(fake pw)\",\"address\":{\"city\":\"Recife\",\"phone\":\"+55 81 0000-0000\"}}" \
    "trace-id:abc-002|cust-2|{\"id\":2,\"name\":\"Bruno\",\"api_token\":\"$(fake tok)\",\"orders\":[{\"sku\":\"A1\",\"card_number\":\"0000 0000 0000 0000\"}]}" \
    "trace-id:abc-003|cust-3|plain text payload, not JSON" |
    "$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
      --topic customers.updated \
      --property parse.headers=true --property headers.delimiter='|' \
      --property headers.separator=, --property headers.key.separator=: \
      --property parse.key=true --property key.separator='|' > /dev/null
  echo "Seeded customers.updated."
fi

if topic_is_empty orders.created.DLQ; then
  printf '%s\n' \
    'error.class:InvalidOrderException,error.message:order total is negative,original.offset:17|key-17|{"seq":17,"total":-5}' \
    'error.class:DeserializationException,error.message:unexpected end of input,original.offset:42|key-42|{"seq":42,"tot' |
    "$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
      --topic orders.created.DLQ \
      --property parse.headers=true --property headers.delimiter='|' \
      --property headers.separator=, --property headers.key.separator=: \
      --property parse.key=true --property key.separator='|' > /dev/null
  echo "Seeded orders.created.DLQ."
fi

echo "Topics:"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list
