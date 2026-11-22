# Security

## Read-only by design

StreamSage only reads cluster metadata. It does not implement producing
messages, creating, deleting or altering topics, changing offsets, or altering
configuration. A contract test checks that the Kafka adapters expose no
write-style methods.

Do not rely on the code alone. Give StreamSage a Kafka principal with the
least privilege that M1 needs:

| Resource | Operation  | Why                                                    |
| -------- | ---------- | ------------------------------------------------------ |
| Cluster  | `Describe` | Cluster and broker metadata; listing consumer groups   |
| Topic(s) | `Describe` | Topic and partition metadata; log end offsets for lag  |
| Group(s) | `Describe` | Describing groups and reading their committed offsets  |
| Topic(s) | `Read`     | **Only** for `inspect_messages` and `find_messages_by_key`; omit it to disable message reads |

Without `Read`, every tool except `inspect_messages` and
`find_messages_by_key` works (`list_dlq_topics` only reads offsets). Topics and groups
the principal cannot describe do not appear in results.

Message inspection uses a consumer with a fixed `group.id`
(`streamsage-inspector-readonly`), because librdkafka requires one. It only
ever calls `assign()` with explicit offsets: it never subscribes, joins the
group or commits offsets, and the group does not appear on the cluster
(verified against Kafka 4.3). This was not tested against a broker that
enforces ACLs. If yours requires `Read` on that group anyway, grant it, or
please report it.

## Credentials

- Credentials are read from environment variables (or a local `.env` that is
  never committed). MCP clients cannot pass credentials in tool calls.
- The password is a `SecretStr`. Its value is extracted only when the Kafka
  client configuration is built, and that configuration is never logged.
- If configuration is invalid, the server exits with field names and reasons
  only, never the configured values.

## Transport and TLS

- `PLAINTEXT` and `SASL_PLAINTEXT` send traffic, including SASL/PLAIN
  passwords, unencrypted. Use them only for local development. StreamSage
  logs a warning when the bootstrap servers are not loopback addresses and
  TLS is off.
- For remote clusters, use `SASL_SSL` (or `SSL`). Set
  `STREAMSAGE_KAFKA_SSL_CA_LOCATION` only if the brokers use a private CA.
- Supported SASL mechanisms: `PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512`.
  `GSSAPI` and `OAUTHBEARER` are not supported yet.
- SCRAM over `SASL_PLAINTEXT` and `SASL_SSL` is tested against a real broker
  (`tests/security`, see [development.md](development.md)).
- `STREAMSAGE_KAFKA_SSL_CA_LOCATION` must point to an existing file; the
  server refuses to start otherwise. If librdkafka rejects the security
  configuration, the server exits with code 2 and a message that names only
  the error code.
- A wrong password is reported as "Authentication with the Kafka cluster
  failed", and an untrusted broker certificate as "TLS handshake ... failed".
  Neither message includes credentials or raw library output.

## MCP trust considerations

- M1 serves only the **stdio** transport. The MCP client launches the server
  as a local subprocess, so whoever can run that client can run the tools.
- All tool arguments are validated against typed schemas. Topic names must
  match Kafka's naming rules (`[a-zA-Z0-9._-]`, 1–249 characters). No input
  reaches a shell or a file path.
- Errors returned to clients contain short, StreamSage-authored messages
  with Kafka error codes. They never include stack traces or raw library
  output.
- Streamable HTTP will not be exposed beyond localhost until authentication
  and access control are in place (M5).

## Data protection

`inspect_messages` can return message data to the MCP client, and from there
to an LLM. Defaults are conservative:

- **Payloads are omitted** unless `STREAMSAGE_MESSAGE_PAYLOADS_ENABLED=true`.
  Keys, headers, timestamps, offsets and payload sizes are returned, since
  the spec treats them as metadata.
- **Redaction by name.** In JSON payloads, the value of any field whose
  normalized name contains a sensitive term (`password`, `secret`, `token`,
  `authorization`, `apikey`, `credential`, `cookie`, `email`, `phone`,
  `cpf`, `cnpj`, `ssn`, `creditcard`, `cardnumber`, `cvv`, `iban`, …) is
  replaced with `[REDACTED]`, recursively. Header values with such names are
  withheld too. Add terms with `STREAMSAGE_MESSAGE_SENSITIVE_FIELDS`.
- **Limits.** At most `STREAMSAGE_MESSAGE_MAX_LIMIT` messages per call
  (default 100, default request 20). Payload previews are capped at
  `STREAMSAGE_MESSAGE_MAX_PAYLOAD_BYTES` (default 1 KB), and keys and header
  values at 256 bytes. Redaction happens before truncation, so a redacted
  value never reappears.
- **Binary content** (Avro, Protobuf, anything that is not UTF-8) is never
  decoded; only its size is returned.
- **No logging.** Message keys, headers and payloads are never logged, and
  raw messages do not render their payload even in debug representations.

`find_messages_by_key` applies the same sanitization to its matches. It
scans at most `STREAMSAGE_MESSAGE_KEY_SEARCH_MAX_TOTAL` messages (default
10,000), split across partitions and capped per partition (default 1,000).
Non-matching messages are discarded as they are read. Memory is therefore
bounded by the number of results, not by the number of messages scanned.

Known limits, stated on every response through `payload_treatment`:

- Redaction relies on **field names**. A sensitive value under an innocuous
  name (for example `"note": "my password is …"`) is not detected.
- Non-JSON text payloads (`text_truncated`) are only truncated, not redacted.
- Keys are returned as metadata and are not redacted; avoid putting personal
  data in keys, or leave `Read` permission off.
