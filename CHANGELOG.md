# Changelog

All notable changes to StreamSage are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Documentation
- New user guides: getting started (release package vs source checkout),
  connecting clients (Claude Code, Claude Desktop, MCP Inspector, HTTP),
  usage with example questions and walkthroughs, a tool reference with real
  responses from the sample environment, and a full configuration reference.
- Mermaid diagrams for the architecture, request flows, security and CI.
- `SECURITY.md` with the vulnerability reporting policy.
- `PROJECT_SPEC.md` translated to English.
- Claude Desktop examples split into release and source variants.

## [1.0.0] - 2026-11-22

First stable release. StreamSage is a read-only MCP server for Apache Kafka:
it lets AI agents and other MCP clients explore clusters, inspect consumers
and investigate problems, with deterministic, evidence-based diagnostics.

### Cluster explorer
- `get_cluster_info`, `list_topics` and `describe_topic` tools, plus the
  `kafka://cluster/info` and `kafka://topics` resources.
- `investigate_kafka_cluster` prompt for guided, evidence-based investigations.
- Topics are never created as a side effect, even on brokers with topic
  auto-creation enabled.

### Consumer intelligence
- `list_consumer_groups`, `describe_consumer_group` and `get_consumer_lag`.
- Unknown lag is reported as `null`, never as 0.
- `analyze_consumer_health`: healthy, warning, critical or unknown
  classification with configurable lag thresholds.

### Diagnostics engine
- Structured findings with evidence, hypotheses and recommendations. Findings
  never assert a root cause.
- Severity overrides per finding code (`STREAMSAGE_FINDING_SEVERITY_OVERRIDES`).
- `detect_stalled_partitions`: compares committed offsets across two samples
  taken within one call.

### Message intelligence
- `inspect_messages` and `find_messages_by_key`, bounded in count and size.
  Payloads are omitted unless enabled; sensitive JSON fields and headers are
  redacted, and message contents are never logged.
- `list_dlq_topics` for operator-configured dead letter queue names and glob
  patterns.

### Production readiness
- Streamable HTTP transport with static bearer tokens (stored as SHA-256
  hashes) and two scopes, `kafka:metadata` and `kafka:messages`.
- `streamsage generate-token` command.
- Prometheus `/metrics` and a public `/healthz` endpoint in HTTP mode.
- SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) and TLS, tested against a real
  secured broker.
- Structured JSON logging on stderr, with MCP correlation ids.

### Security
- Read-only by design: no tool can produce messages, change offsets, or
  create, delete or alter topics or configuration.
- Documented least-privilege ACLs: `Describe` on cluster, topics and groups,
  plus `Read` on topics only when message inspection is wanted.

[Unreleased]: https://github.com/FVitor7/streamsage/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/FVitor7/streamsage/releases/tag/v1.0.0
