# StreamSage

**AI-powered Kafka observability & troubleshooting MCP server.**

StreamSage is an open-source [Model Context Protocol](https://modelcontextprotocol.io)
server, written in Python, that lets AI agents and other MCP clients explore
Apache Kafka clusters: cluster metadata, topics, partitions, leaders and
in-sync replicas. It is **read-only by design**.

> Status: **M4 — Message Intelligence**: bounded, sanitized message
> inspection, key search and configurable DLQ topics. See the
> [roadmap](#roadmap).

## Why

The information needed to operate Kafka is spread across topics, partitions,
brokers, consumer groups, offsets and configs, usually reached through several
CLI tools. StreamSage puts that information behind one consistent, typed,
read-only interface. Your MCP client (Claude Desktop, Cursor, …) runs the
tools and reasons over the results.

The server does not call any LLM. It returns data, and in later milestones it
will also return deterministic, evidence-based diagnostics.

## Features

| Kind     | Name                        | Description                                                     |
| -------- | --------------------------- | --------------------------------------------------------------- |
| Tool     | `get_cluster_info`          | Cluster id, brokers (id/host/port), controller id               |
| Tool     | `list_topics`               | Topics with partition count and replication factor; `include_internal: bool` |
| Tool     | `describe_topic`            | Per-partition leader, replicas and ISR; under-replicated/leaderless counts |
| Tool     | `list_consumer_groups`      | Consumer groups with their state; optional `states` filter      |
| Tool     | `describe_consumer_group`   | State, assignor, coordinator, members and their partitions      |
| Tool     | `get_consumer_lag`          | Per-partition lag, largest first, with total/max lag; `max_partitions` |
| Tool     | `analyze_consumer_health`   | healthy / warning / critical / unknown, with evidence-backed findings and limitations |
| Tool     | `detect_stalled_partitions` | Samples offsets twice (`interval_seconds`, 1–60) and reports partitions that did not advance |
| Tool     | `inspect_messages`          | Newest (or from an offset) messages: key, headers, timestamp, offset, size; payload only if enabled, sanitized |
| Tool     | `find_messages_by_key`      | Newest messages with an exact key, scanning bounded ranges and reporting coverage |
| Tool     | `list_dlq_topics`           | Topics matching `STREAMSAGE_DLQ_TOPICS`, with approximate message counts |
| Resource | `kafka://cluster/info`      | Cluster info as JSON                                            |
| Resource | `kafka://topics`            | Topic list as JSON                                              |
| Prompt   | `investigate_kafka_cluster` | Guided, evidence-based investigation (optional `focus_topic`)   |

Every tool has a typed input schema and structured output, and is annotated
`readOnlyHint: true`. Internal topics (`__*`) are hidden by default.
`describe_topic` never creates topics, even on brokers that have
auto-creation enabled; the integration tests verify this.

Lag is reported per partition as `log_end_offset - committed_offset`. A
partition the group has never committed has `lag: null`. StreamSage never
reports unknown lag as 0. Lag is a single snapshot: high lag alone does not
prove a consumer is stuck. `detect_stalled_partitions` checks progress by
sampling committed offsets twice within one call.

Every diagnostic finding states what was observed and the evidence behind
it. Possible explanations go in a separate `hypotheses` list, and next steps
in `recommendations`. No finding asserts a root cause.

## Architecture

Clean Architecture with Ports and Adapters. Dependencies point inward, and the
domain is plain Python with no Kafka, MCP or Pydantic imports.

```text
MCP client ──stdio──▶ Presentation (MCP tools/resources/prompts)
                            │
                            ▼
                      Application (use cases, ports)
                            │
                ┌───────────┴───────────┐
                ▼                       ▼
         Domain (entities,       Infrastructure (Kafka adapters,
         rules, errors)          confluent-kafka, settings)
                                        │
                                        ▼
                                  Apache Kafka
```

See [docs/architecture.md](docs/architecture.md) for details and decisions.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker (for the local Kafka broker)

## Quick start

```bash
# 1. Start a local Kafka broker (KRaft, single node) and create sample topics
docker compose up -d --wait kafka
docker compose run --rm kafka-init

# 2. Install dependencies
uv sync

# 3. Run the MCP server (stdio). Normally your MCP client launches it for you.
uv run streamsage
```

Explore it interactively with the MCP Inspector:

```bash
npx @modelcontextprotocol/inspector uv run --directory "$PWD" streamsage
```

Or call tools from the command line with the Inspector CLI:

```bash
npx @modelcontextprotocol/inspector --cli uv run --directory "$PWD" streamsage \
  --method tools/call --tool-name describe_topic --tool-arg topic_name=orders.created
```

A web UI for the local cluster (kafbat/kafka-ui) runs at http://localhost:8080
after `docker compose up -d`.

Stop Kafka with `docker compose down` (add `-v` to delete its data volume).

## Kafka configuration

StreamSage reads its configuration from environment variables prefixed with
`STREAMSAGE_`, or from a `.env` file in the working directory. See
[.env.example](.env.example).

| Variable                              | Default          | Notes                                                |
| ------------------------------------- | ---------------- | ---------------------------------------------------- |
| `STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS`  | `localhost:9092` | Comma-separated `host:port` list                     |
| `STREAMSAGE_KAFKA_SECURITY_PROTOCOL`  | `PLAINTEXT`      | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`     |
| `STREAMSAGE_KAFKA_SASL_MECHANISM`     | –                | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`            |
| `STREAMSAGE_KAFKA_SASL_USERNAME`      | –                | Required with `SASL_*`                               |
| `STREAMSAGE_KAFKA_SASL_PASSWORD`      | –                | Required with `SASL_*`; never logged                 |
| `STREAMSAGE_KAFKA_SSL_CA_LOCATION`    | –                | CA bundle for `SSL`/`SASL_SSL` (defaults to system)  |
| `STREAMSAGE_KAFKA_REQUEST_TIMEOUT`    | `10`             | Seconds; upper bound for each metadata request       |
| `STREAMSAGE_KAFKA_SOCKET_TIMEOUT`     | `10`             | Seconds                                              |
| `STREAMSAGE_LOG_LEVEL`                | `INFO`           | Logs go to stderr as JSON lines                      |
| `STREAMSAGE_INCLUDE_INTERNAL_TOPICS`  | `false`          | Default for `list_topics` and `kafka://topics`       |
| `STREAMSAGE_LAG_WARNING_THRESHOLD`    | `1000`           | Per-partition lag (messages) for a `warning` finding |
| `STREAMSAGE_LAG_CRITICAL_THRESHOLD`   | `10000`          | Per-partition lag for `critical`; must be ≥ warning  |
| `STREAMSAGE_FINDING_SEVERITY_OVERRIDES` | `{}`           | JSON map of finding code → `info`/`warning`/`critical` |
| `STREAMSAGE_MESSAGE_PAYLOADS_ENABLED` | `false`          | Return sanitized payload previews (otherwise size only) |
| `STREAMSAGE_MESSAGE_MAX_PAYLOAD_BYTES` | `1024`          | Payload preview limit (1–65536)                      |
| `STREAMSAGE_MESSAGE_DEFAULT_LIMIT`    | `20`             | Messages per `inspect_messages` call by default      |
| `STREAMSAGE_MESSAGE_MAX_LIMIT`        | `100`            | Maximum messages per call (≤ 1000)                   |
| `STREAMSAGE_MESSAGE_SENSITIVE_FIELDS` | –                | Extra field/header names to redact, comma-separated  |
| `STREAMSAGE_MESSAGE_KEY_SEARCH_MAX_PER_PARTITION` | `1000` | Newest messages scanned per partition by key search |
| `STREAMSAGE_MESSAGE_KEY_SEARCH_MAX_TOTAL` | `10000`      | Total key search budget, split across partitions     |
| `STREAMSAGE_DLQ_TOPICS`               | –                | DLQ topic names/glob patterns, e.g. `*.DLQ,dead-letter-*` |

The server checks for inconsistent settings at startup, for example a SASL
protocol without credentials, or credentials without a SASL protocol. If it
finds any, it exits with a message that never includes configured values.
`PLAINTEXT` is meant for local development only; the server logs a warning
when traffic to a non-loopback broker would be unencrypted.

Example for a managed cluster using SASL/SCRAM over TLS:

```bash
STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS=broker-1.example.com:9096,broker-2.example.com:9096
STREAMSAGE_KAFKA_SECURITY_PROTOCOL=SASL_SSL
STREAMSAGE_KAFKA_SASL_MECHANISM=SCRAM-SHA-512
STREAMSAGE_KAFKA_SASL_USERNAME=streamsage-readonly
STREAMSAGE_KAFKA_SASL_PASSWORD=...
```

## MCP client configuration

Claude Desktop (`claude_desktop_config.json`); see
[examples/claude_desktop.json](examples/claude_desktop.json):

```json
{
  "mcpServers": {
    "streamsage": {
      "command": "uv",
      "args": ["run", "--directory", "/absolute/path/to/streamsage", "streamsage"],
      "env": { "STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092" }
    }
  }
}
```

Claude Code:

```bash
claude mcp add streamsage -- uv run --directory /absolute/path/to/streamsage streamsage
```

Then ask things like *"What topics are available in this cluster?"*,
*"Which consumer groups have the highest lag?"* or *"Investigate the
`orders.created` topic."*

## Security model

- **Read-only.** No code path produces messages, creates or deletes topics,
  changes offsets or alters configuration. Also enforce this with Kafka ACLs:
  give StreamSage `Describe` permissions, plus `Read` on topics only if you
  want `inspect_messages`.
- **Message privacy.** Payloads are omitted unless
  `STREAMSAGE_MESSAGE_PAYLOADS_ENABLED=true`. Even then, JSON fields and
  headers with sensitive names are redacted and previews are truncated.
  Message contents are never logged. Message inspection never joins a
  consumer group or commits offsets.
- **Credentials come from the operator**, through the environment. MCP clients
  cannot supply them per call.
- **No secret leakage.** Passwords are `SecretStr`. Logs, tool errors and
  resources never include credentials, and clients never see stack traces.
- **Bounded.** Every Kafka call has an explicit timeout.
- **stdio transport only** in M1. Streamable HTTP (with authentication) is
  planned for M5.

See [docs/security.md](docs/security.md).

## Development and tests

```bash
make check             # ruff lint + format check, mypy (strict), unit tests with coverage
make kafka-up          # start Kafka and create sample topics
make test-integration  # tests against the real broker, including an end-to-end stdio test
make kafka-secure-up   # secured broker (SASL/SCRAM + TLS) with generated test credentials
make test-security     # SASL/TLS tests, including stdio over SASL_SSL
```

Unit tests need no broker; integration tests are marked `integration` and are
excluded by default. See [docs/development.md](docs/development.md).

## Roadmap

- **M1 — Foundation & Cluster Explorer** ✅ cluster info, topics, partitions
- **M2 — Consumer Intelligence** ✅ consumer groups, lag, health analysis
  with configurable thresholds
- **M3 — Diagnostics Engine** ✅ structured findings with evidence,
  hypotheses and recommendations; configurable severity; stalled-partition
  detection by comparing offsets over time
- **M4 — Message Intelligence** ✅ bounded, sanitized message inspection,
  key search and configurable DLQ topics
- **M5 — Production Readiness**: Streamable HTTP with auth, metrics, v1.0

## Contributing

Issues and pull requests are welcome. Please run `make check` (and
`make test-integration` for Kafka-facing changes) before opening a PR, and
keep the dependency rule: no Kafka or MCP imports in `domain/` or
`application/`.

## License

[MIT](LICENSE)
