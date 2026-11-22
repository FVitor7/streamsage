# Security

## Read-only by design

StreamSage only reads cluster metadata. It does not implement producing
messages, creating, deleting or altering topics, changing offsets, or altering
configuration. A contract test checks that the Kafka adapters expose no
write-style methods.

Do not rely on the code alone. Give StreamSage a Kafka principal with the
least privilege that M1 needs:

| Resource | Operation  | Why                          |
| -------- | ---------- | ---------------------------- |
| Cluster  | `Describe` | Cluster and broker metadata  |
| Topic(s) | `Describe` | Topic and partition metadata |

Topics the principal cannot describe do not appear in results. Later
milestones (consumer groups, lag) will need `Describe` on groups and will be
documented when added.

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

M1 never reads message payloads. Message inspection (M4) will be opt-in,
bounded in count and size, and sanitized.
