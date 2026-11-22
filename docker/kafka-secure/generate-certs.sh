#!/usr/bin/env bash
# Runs INSIDE the apache/kafka image (which ships keytool) to create a
# throwaway CA and a broker certificate for local TLS testing.
#
# Input:  /out/keystore_creds (keystore password, created by setup.sh)
# Output: /out/ca.pem, /out/broker.keystore.p12, /out/key_creds
#
# Everything here is disposable test material, regenerated on demand and
# never committed (see .gitignore).
set -euo pipefail

OUT=/out
PASS="$(cat "$OUT/keystore_creds")"
WORK="$(mktemp -d)"
DAYS=30

keytool() { /opt/java/openjdk/bin/keytool "$@"; }

# 1. Certificate authority.
keytool -genkeypair -alias ca -keyalg RSA -keysize 2048 -validity "$DAYS" \
  -dname "CN=StreamSage Local Test CA" -ext bc:c \
  -keystore "$WORK/ca.p12" -storetype PKCS12 -storepass "$PASS" -noprompt
keytool -exportcert -rfc -alias ca -keystore "$WORK/ca.p12" -storepass "$PASS" \
  > "$OUT/ca.pem"

# 2. Broker key pair, signed by the CA. SANs cover host and container access.
keytool -genkeypair -alias broker -keyalg RSA -keysize 2048 -validity "$DAYS" \
  -dname "CN=localhost" \
  -keystore "$OUT/broker.keystore.p12" -storetype PKCS12 -storepass "$PASS" -noprompt
keytool -certreq -alias broker -keystore "$OUT/broker.keystore.p12" \
  -storepass "$PASS" -file "$WORK/broker.csr"
keytool -gencert -alias ca -keystore "$WORK/ca.p12" -storepass "$PASS" \
  -infile "$WORK/broker.csr" -outfile "$WORK/broker.crt" -rfc -validity "$DAYS" \
  -ext "SAN=DNS:localhost,DNS:kafka-secure,IP:127.0.0.1" -ext "EKU=serverAuth"

# 3. Import the chain (CA first, then the signed broker certificate).
keytool -importcert -alias ca -file "$OUT/ca.pem" \
  -keystore "$OUT/broker.keystore.p12" -storepass "$PASS" -noprompt
keytool -importcert -alias broker -file "$WORK/broker.crt" \
  -keystore "$OUT/broker.keystore.p12" -storepass "$PASS" -noprompt

# PKCS12 uses the store password for the key too.
cp "$OUT/keystore_creds" "$OUT/key_creds"
rm -rf "$WORK"
echo "Generated CA and broker keystore in $OUT"
