#!/usr/bin/env bash
# Starts the dev-only secured Kafka broker used by the `security` tests.
#
#   localhost:9094  SASL_PLAINTEXT  (SCRAM-SHA-512)
#   localhost:9095  SASL_SSL        (SCRAM-SHA-512 over TLS, private test CA)
#
# Generated material lives in docker/kafka-secure/.generated/ (gitignored):
#   sasl_password   password of the SCRAM user "streamsage"
#   ca.pem          CA certificate clients must trust for port 9095
#   broker.keystore.p12, keystore_creds, key_creds   broker TLS material
#
# Re-running is safe. Delete .generated/ and run `docker compose --profile
# security down -v` to start from scratch.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GEN="$ROOT/docker/kafka-secure/.generated"
SCRAM_USER="streamsage"

mkdir -p "$GEN"
chmod 700 "$GEN"

new_secret() { openssl rand -hex 24; }

[[ -s "$GEN/sasl_password" ]] || new_secret > "$GEN/sasl_password"

if [[ ! -s "$GEN/broker.keystore.p12" ]]; then
  new_secret > "$GEN/keystore_creds"
  docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v "$GEN:/out" \
    -v "$ROOT/docker/kafka-secure/generate-certs.sh:/generate-certs.sh:ro" \
    --entrypoint bash \
    apache/kafka:4.3.2 /generate-certs.sh
fi
# The broker runs as uid 1000 and must be able to read its material.
chmod 755 "$GEN"
chmod 644 "$GEN"/*

cd "$ROOT"
docker compose --profile security up -d --wait kafka-secure

# Create (or update) the SCRAM user through the broker's internal listener.
docker compose exec -T kafka-secure /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-secure:19092 \
  --alter --entity-type users --entity-name "$SCRAM_USER" \
  --add-config "SCRAM-SHA-512=[password=$(cat "$GEN/sasl_password")]" > /dev/null

echo "Secured broker ready: SASL_PLAINTEXT localhost:9094, SASL_SSL localhost:9095"
echo "SCRAM user: $SCRAM_USER (password in $GEN/sasl_password)"
