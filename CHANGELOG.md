# Changelog

All notable changes to StreamSage are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.1] - 2026-11-23

Release and CI hardening. No change to StreamSage's behavior.

### Added
- Signed build provenance attestations for release files. Verify a
  download with
  `gh attestation verify <file> --repo FVitor7/streamsage`.
- `scripts/smoke_test_package.py`. It installs a built wheel or sdist into a
  clean environment and checks:
  - the version and `py.typed`;
  - the CLI;
  - an MCP handshake over stdio;
  - that the native Kafka client loads.

  CI runs it on every pull request (wheel on Python 3.12 and 3.15, sdist on
  3.12), and the release workflow runs it before publishing.
- A CI job that runs the unit tests on the newest Python (3.15).
- Documentation checks in the unit tests:
  - relative links and anchors resolve;
  - example client configs are valid;
  - every setting is documented;
  - no document mentions a setting that does not exist.

### Changed
- Dependabot uses the `uv` ecosystem, so updates include `uv.lock` (the
  `pip` ecosystem left it stale, failing `uv sync --locked`). It also tracks
  the `docker-compose.yml` images, and groups dev-tool and GitHub Actions
  updates.
- Package metadata lists Python 3.13, 3.14 and 3.15 as supported, in
  addition to 3.12.

## [1.1.0] - 2026-11-23

Hardening release from a technical review. Message reads are bounded in
messages, memory and payload size, and partial or estimated results always
say so. Lag and ordering are described more precisely.

**Upgrading:** backward compatible. Responses only gain fields; nothing was
renamed or removed. The new settings have defaults that every valid 1.0.0
configuration satisfies. One behavior changed on purpose:
`find_messages_by_key` never scans more than
`STREAMSAGE_MESSAGE_KEY_SEARCH_MAX_TOTAL` messages, and on topics with more
partitions than that it reports the ones it skipped.

### Added
- `STREAMSAGE_MESSAGE_KEYS_ENABLED` (default `true`). Set it to `false` to
  withhold message keys from `inspect_messages` and `find_messages_by_key`
  responses, for topics whose keys carry personal data. Messages gain a
  `key_withheld` field.

### Changed
- Lag is now described as **offset lag** everywhere: tool descriptions,
  findings, health limitations and docs say "offsets behind" instead of
  "pending messages". On compacted or transactional topics, offset lag is
  an upper bound on unconsumed messages. `STREAMSAGE_LAG_*_THRESHOLD` are
  documented in offsets. No fields were renamed and values are unchanged.
- `confluent_kafka.admin` types are imported from their public module
  instead of private submodules (`_group`, `_metadata`, …), so library
  upgrades that reorganize internals do not break StreamSage.

### Fixed
- Messages without a timestamp were ranked as if they were from 1970, and
  messages of one partition could be reordered by producer timestamps that
  go backwards. Within a partition, the offset now always decides the
  order. A message without a timestamp is ranked as if it had the nearest
  earlier timestamp in its partition. When such messages are read from more
  than one partition, `inspect_messages` returns `exact: false`, and both
  message tools add a note that the order across partitions is an
  estimate.
- `list_consumer_groups` returned a partial list without saying so when some
  brokers failed to answer. Responses now include `complete`,
  `failed_broker_count`, `failure_codes` and a note that a missing group may
  still exist.
- `find_messages_by_key` could scan more than
  `STREAMSAGE_MESSAGE_KEY_SEARCH_MAX_TOTAL` messages on topics with more
  partitions than the budget, because each partition got at least one
  message. The budget is now a hard limit: the first partitions by id are
  searched and the rest are reported in `uncovered_partitions`. Empty
  partitions no longer use any budget. Responses also report
  `unavailable_partitions`.
- `inspect_messages` could read up to `limit` messages from **every**
  partition and hold them all in memory (for example 50,000 messages for
  `limit=100` on 500 partitions). It now keeps only the `limit` newest while
  reading, and enforces a global read budget
  (`STREAMSAGE_MESSAGE_INSPECT_MAX_READ`, default 10,000). Results report
  `exact` (whether they are guaranteed to be the newest) and
  `messages_read`.
- Payloads of any size were kept in memory and parsed with `json.loads` to
  build a 1 KB preview. Payloads above `STREAMSAGE_MESSAGE_MAX_DECODE_BYTES`
  (default 256 KiB) are now dropped as they are read and reported as
  `too_large`. Keys and headers decode only the prefix they need.
- Partition offsets were fetched with one request per partition, each with
  its own timeout. They now use two batched ListOffsets requests per topic,
  which also speeds up `find_messages_by_key` and `list_dlq_topics`.
  Partitions whose offsets cannot be read are reported as unavailable
  instead of failing the whole call.

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

[Unreleased]: https://github.com/FVitor7/streamsage/compare/v1.1.1...HEAD
[1.1.1]: https://github.com/FVitor7/streamsage/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/FVitor7/streamsage/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/FVitor7/streamsage/releases/tag/v1.0.0
