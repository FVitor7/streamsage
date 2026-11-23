# Configuration

StreamSage is configured with environment variables prefixed with
`STREAMSAGE_`. Configuration is validated at startup. An invalid or
inconsistent setting stops the server with exit code 2 and a message that
names the variable, never its value.

## Where to put settings

| Setup | Recommended place |
| --- | --- |
| MCP client over stdio (release or source) | The client's server config: `-e KEY=value` for `claude mcp add`, the `env` object for Claude Desktop |
| Source checkout, local development | A `.env` file in the repository root (gitignored); see [`.env.example`](../.env.example) |
| HTTP server, container, systemd | The process environment, with secrets from your secret manager |

StreamSage also reads a `.env` file from its **working directory**. MCP
clients choose that directory, so for an installed package prefer the
client's `env` settings. Never commit real credentials.

## Kafka connection

| Variable | Default | Allowed values | Notes |
| --- | --- | --- | --- |
| `STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `host:port[,host:port…]` | Validated format |
| `STREAMSAGE_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` | `PLAINTEXT`/`SASL_PLAINTEXT` to a non-loopback host logs a warning |
| `STREAMSAGE_KAFKA_SASL_MECHANISM` | – | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` | Required with `SASL_*` |
| `STREAMSAGE_KAFKA_SASL_USERNAME` | – | string | Required with `SASL_*` |
| `STREAMSAGE_KAFKA_SASL_PASSWORD` | – | string (secret) | Required with `SASL_*`; never logged or returned |
| `STREAMSAGE_KAFKA_SSL_CA_LOCATION` | system trust store | path to a PEM CA bundle | Only with `SSL`/`SASL_SSL`; must be an existing file |
| `STREAMSAGE_KAFKA_REQUEST_TIMEOUT` | `10` | seconds, `0 < x ≤ 300` | Upper bound for each Kafka request and message read |
| `STREAMSAGE_KAFKA_SOCKET_TIMEOUT` | `10` | seconds, `0 < x ≤ 300` | librdkafka socket timeout |

Consistency rules:

| Rule | Example of a rejected configuration |
| --- | --- |
| `SASL_*` protocols need mechanism, username and password | `SASL_SSL` without `STREAMSAGE_KAFKA_SASL_PASSWORD` |
| SASL settings need a `SASL_*` protocol | `STREAMSAGE_KAFKA_SASL_USERNAME` set with `PLAINTEXT` |
| A CA bundle needs a TLS protocol | `STREAMSAGE_KAFKA_SSL_CA_LOCATION` set with `SASL_PLAINTEXT` |

`GSSAPI` (Kerberos), `OAUTHBEARER` and client certificates (mTLS) are not
supported yet.

## Transport

| Variable | Default | Allowed values | Notes |
| --- | --- | --- | --- |
| `STREAMSAGE_TRANSPORT` | `stdio` | `stdio`, `http` | `http` serves Streamable HTTP at `/mcp` |
| `STREAMSAGE_HTTP_HOST` | `127.0.0.1` | host or IP | Binding to a non-loopback address logs a TLS warning |
| `STREAMSAGE_HTTP_PORT` | `8000` | `1–65535` | |
| `STREAMSAGE_HTTP_TOKENS` | `[]` | JSON list of `{"name", "sha256", "scopes"}` | Required for `http`. Create entries with `streamsage generate-token` |
| `STREAMSAGE_HTTP_METRICS_PUBLIC` | `false` | boolean | Serve `/metrics` without a token |

Token entry rules: `name` uses `[A-Za-z0-9._-]` (1–64 chars) and must be
unique; `sha256` is 64 lowercase hex chars; `scopes` is a non-empty subset
of `kafka:metadata` and `kafka:messages`. See
[clients.md](clients.md#http-mode-shared-server).

## Topics and logging

| Variable | Default | Notes |
| --- | --- | --- |
| `STREAMSAGE_INCLUDE_INTERNAL_TOPICS` | `false` | Default for `list_topics` and `kafka://topics` |
| `STREAMSAGE_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Logs are JSON lines on stderr |

## Diagnostics

| Variable | Default | Notes |
| --- | --- | --- |
| `STREAMSAGE_LAG_WARNING_THRESHOLD` | `1000` | Per-partition lag (messages) for `LAG_ABOVE_WARNING` |
| `STREAMSAGE_LAG_CRITICAL_THRESHOLD` | `10000` | Per-partition lag for `LAG_ABOVE_CRITICAL`; must be ≥ warning |
| `STREAMSAGE_FINDING_SEVERITY_OVERRIDES` | `{}` | JSON map of finding code → `info`, `warning` or `critical` |

The thresholds are starting points: tune them to each consumer's normal
throughput. Overrides change how findings count toward the status, for
example:

```bash
# Inactive groups are expected here; stalled partitions are an emergency
STREAMSAGE_FINDING_SEVERITY_OVERRIDES='{"NO_ACTIVE_MEMBERS": "info", "PARTITIONS_NOT_ADVANCING": "critical"}'
```

Unknown codes are rejected at startup. Valid codes are listed in
[tools.md](tools.md#analyze_consumer_health) and
[tools.md](tools.md#detect_stalled_partitions).

## Message inspection

| Variable | Default | Range | Notes |
| --- | --- | --- | --- |
| `STREAMSAGE_MESSAGE_PAYLOADS_ENABLED` | `false` | boolean | Without it, responses show keys, headers and sizes only |
| `STREAMSAGE_MESSAGE_KEYS_ENABLED` | `true` | boolean | Set to `false` when keys carry personal data: keys are then withheld (`key_withheld: true`), with only their size shown |
| `STREAMSAGE_MESSAGE_MAX_PAYLOAD_BYTES` | `1024` | `1–65536` | Payload preview limit, applied after redaction |
| `STREAMSAGE_MESSAGE_DEFAULT_LIMIT` | `20` | ≥ 1 | Default `limit` for `inspect_messages` and `find_messages_by_key` |
| `STREAMSAGE_MESSAGE_MAX_LIMIT` | `100` | `1–1000` | Maximum `limit`; must be ≥ the default |
| `STREAMSAGE_MESSAGE_INSPECT_MAX_READ` | `10000` | `1–1000000` | Messages `inspect_messages` may read per call, across partitions; must be ≥ the max limit. Beyond it the result is marked `exact: false` |
| `STREAMSAGE_MESSAGE_MAX_DECODE_BYTES` | `262144` | `1–16777216` | Payloads larger than this are neither kept in memory nor decoded (`too_large`); must be ≥ the preview limit |
| `STREAMSAGE_MESSAGE_SENSITIVE_FIELDS` | – | comma-separated names | Extra field and header names to redact, added to the built-in list |
| `STREAMSAGE_MESSAGE_KEY_SEARCH_MAX_PER_PARTITION` | `1000` | `1–100000` | Newest messages scanned per partition by key search |
| `STREAMSAGE_MESSAGE_KEY_SEARCH_MAX_TOTAL` | `10000` | `1–1000000` | Hard limit on messages scanned per key search, split across non-empty partitions. With more partitions than this, the rest are reported as uncovered |
| `STREAMSAGE_DLQ_TOPICS` | – | comma-separated names and globs | Topics treated as DLQs, e.g. `*.DLQ,dead-letter-*` (case-sensitive) |

The built-in sensitive terms are `password`, `passwd`, `pwd`, `secret`,
`token`, `authorization`, `apikey`, `accesskey`, `privatekey`, `credential`,
`cookie`, `cpf`, `cnpj`, `ssn`, `email`, `phone`, `creditcard`,
`cardnumber`, `cvv` and `iban`. They match field names case-insensitively,
ignoring separators, so `userPassword`, `api_key` and `X-Api-Key` all match.

## Ready-made profiles

### Local sample environment

```bash
STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
STREAMSAGE_DLQ_TOPICS=*.DLQ
STREAMSAGE_MESSAGE_PAYLOADS_ENABLED=true
```

### Managed cluster with SASL/SCRAM over TLS

```bash
STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS=broker-1.example.com:9096,broker-2.example.com:9096
STREAMSAGE_KAFKA_SECURITY_PROTOCOL=SASL_SSL
STREAMSAGE_KAFKA_SASL_MECHANISM=SCRAM-SHA-512
STREAMSAGE_KAFKA_SASL_USERNAME=streamsage-readonly
STREAMSAGE_KAFKA_SASL_PASSWORD=<from your secret manager>
```

### TLS with a private certificate authority

```bash
STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS=kafka.internal:9093
STREAMSAGE_KAFKA_SECURITY_PROTOCOL=SSL
STREAMSAGE_KAFKA_SSL_CA_LOCATION=/etc/streamsage/kafka-ca.pem
```

### Shared HTTP server, metadata only

```bash
STREAMSAGE_TRANSPORT=http
STREAMSAGE_HTTP_HOST=0.0.0.0          # behind a TLS-terminating reverse proxy
STREAMSAGE_HTTP_PORT=8000
STREAMSAGE_HTTP_TOKENS='[{"name": "team-sre", "sha256": "<hash>", "scopes": ["kafka:metadata"]}]'
# Leave message inspection off for this server:
STREAMSAGE_MESSAGE_PAYLOADS_ENABLED=false
```

### Large clusters

```bash
STREAMSAGE_KAFKA_REQUEST_TIMEOUT=30
STREAMSAGE_MESSAGE_KEY_SEARCH_MAX_TOTAL=50000
STREAMSAGE_LAG_WARNING_THRESHOLD=50000
STREAMSAGE_LAG_CRITICAL_THRESHOLD=500000
```

Larger budgets mean longer tool calls. Keep each call well below your MCP
client's request timeout.
