# Architecture

StreamSage follows Clean Architecture combined with Ports and Adapters.

```text
src/streamsage/
├── domain/           Pure Python: entities, value objects, rules, errors
├── application/      Use cases and ports (typing.Protocol)
├── infrastructure/   Settings and Kafka adapters (confluent-kafka)
├── presentation/mcp/ MCP server: tools, resources, prompts, response schemas
├── shared/           Cross-cutting: JSON logging
└── main.py           Composition root
```

## Dependency rule

- `domain` imports nothing outside itself and the standard library.
- `application` imports `domain` only. It declares the ports `ClusterReader`
  and `TopicReader`.
- `infrastructure` implements the ports and is the only layer that imports
  `confluent_kafka`.
- `presentation` imports `application` and `domain`, and is the only layer
  that imports `mcp`.
- `main.py` is the only place that creates concrete objects and wires them
  together.

## Request flow

```text
tools/call describe_topic
  → topic_tools.describe_topic        (schema validation by the SDK)
  → operation_boundary                (timing, logging, error mapping)
  → DescribeTopicUseCase.execute      (topic name rules, partition ordering)
  → KafkaTopicAdapter.describe_topic  (TopicReader port)
  → KafkaAdminGateway.request        (AdminClient.describe_topics, bounded wait)
  ← domain Topic → TopicDetailResponse (structured output)
```

## Decisions

**MCP SDK 2.x (`MCPServer`).** In `mcp` 2.x, the v1 `FastMCP` class is renamed
to `mcp.server.mcpserver.MCPServer`. We use its decorators (`tool`,
`resource`, `prompt`) and run it with `run("stdio")`. Tool handlers are plain
synchronous functions; the SDK runs them in a worker thread. As the spec
requires, this keeps the synchronous Kafka client out of asyncio code.

**Topic metadata without accidental creation.** Listings use
`AdminClient.list_topics(timeout=…)` with no topic argument. That single
response contains the cluster id, the controller id, the brokers and every
topic's partitions, leaders, replicas and ISR. Against Kafka 4.3, a Metadata
request scoped to a missing topic name (`list_topics(topic=…)`) *did*
auto-create that topic on a broker with `auto.create.topics.enable=true`, so
StreamSage never issues one.

`describe_topic` uses the DescribeTopics admin API
(`AdminClient.describe_topics`), which requests only the named topic. Its
cost therefore no longer grows with the size of the cluster. It reports
`UNKNOWN_TOPIC_OR_PART` for a missing topic and does not create it; this was
verified against Kafka 4.3 with confluent-kafka 2.16, and
`test_missing_topic_raises_and_is_never_created` guards it on every run. The
local Docker broker keeps auto-creation enabled on purpose.

**One gateway for every Kafka request.** Adapters never call the
`AdminClient` directly; they go through `KafkaAdminGateway`. It applies the
timeout and waits for future-based APIs with a bounded wait. It drains
librdkafka's event queue after each request, and translates errors, mapping
"does not exist" codes to `KafkaResourceNotFoundError`.

**Internal topics.** Cluster metadata, which listings use, does not carry an
"internal" flag. There, topics whose names start with `__` (Kafka's reserved
prefix, for example `__consumer_offsets`) are treated as internal.
`describe_topic` uses the `is_internal` flag returned by DescribeTopics.

**Consumer lag.** `get_consumer_lag` combines three reads:
DescribeConsumerGroups (state, members, assignment),
ListConsumerGroupOffsets (committed offsets) and ListOffsets with
`OffsetSpec.latest()` (log end offsets). It covers every partition the group
has committed *or* is currently assigned. That way a partition that is
assigned but has never been committed shows up with `lag: null`, instead of
being silently omitted. Rules:

- Unknown committed or end offsets give `lag: null`, never 0. `total_lag`
  sums only known lags, and `unknown_lag_partition_count` reports the rest.
- The end offset is the high watermark (read_uncommitted). For consumers
  using `isolation.level=read_committed`, lag may be overstated while
  transactions are open.
- Offsets are read at slightly different moments, so a committed offset
  ahead of the read end offset yields 0 rather than a negative lag.
- A per-partition failure (for example a deleted partition) yields
  `log_end_offset: null` for that partition only. A connection,
  authentication or timeout failure fails the whole call, because partial
  results would mislead.
- Kafka answers DescribeGroups for an unknown id with an empty group in
  state `DEAD`, not an error. StreamSage reports that as "does not exist".
- If some brokers fail to answer a group listing, the groups that were
  returned are still listed and a warning is logged. If all brokers fail,
  the call fails.

**Consumer health.** `analyze_consumer_health` is a pure domain function
(`domain/diagnostics/consumer_health.py`) applied to the group description
and its lag. Each rule produces a `Finding`: a stable `code`, a `severity`,
a summary, and the `evidence` behind it. Possible explanations are worded as
unverified hypotheses.

| Code | Severity | When |
| --- | --- | --- |
| `LAG_ABOVE_CRITICAL` | critical | Some partition lag ≥ critical threshold |
| `LAG_ABOVE_WARNING` | warning | Some partition lag ≥ warning threshold (and none critical) |
| `NO_ACTIVE_MEMBERS` | warning | No members while messages are pending |
| `REBALANCING` | warning | Group is preparing or completing a rebalance |
| `UNASSIGNED_PARTITIONS_WITH_LAG` | warning | Members exist, but partitions with lag are assigned to none |
| `INACTIVE_CAUGHT_UP` | info | No members and nothing pending |
| `LAG_UNKNOWN` | info | Some partitions have unknown lag |
| `NO_PARTITIONS` | info | No committed offsets and no assignments |
| `STATE_UNKNOWN` | info | Coordinator reported no known state |

The overall status is the worst severity found. With no warning or critical
finding, it is `unknown` if no lag could be determined at all, and
`healthy` otherwise. Every report carries its limitations: it is one
snapshot, and the thresholds (`STREAMSAGE_LAG_*_THRESHOLD`, per partition, in
messages) are configured values, not derived from the consumer's throughput.
The analysis never states that a consumer is stuck or names a root cause.

**Findings.** Every diagnostic produces `Finding`s
(`domain/diagnostics/finding.py`) with:

- `code`: stable identifier (the full list is in `diagnostics/catalog.py`);
- `severity`: `info`, `warning` or `critical`;
- `summary`: only what was observed;
- `evidence`: the observed values behind the summary (capped per finding);
- `hypotheses`: possible explanations, unverified by definition;
- `recommendations`: read-only next steps.

Summaries never contain hypotheses, and no finding asserts a root cause.
Insufficient data is reported explicitly, through findings whose summary
starts with "Insufficient data" and a `unknown` status, never as `healthy`.

**Configurable severity.** `STREAMSAGE_FINDING_SEVERITY_OVERRIDES` (JSON)
maps finding codes to severities, for example to treat inactive groups as
`info` or not-advancing partitions as `critical`. The overall status is
computed after overrides. Unknown codes and invalid values are rejected at
startup, so a typo cannot silently disable an override.

**Stalled partitions (`detect_stalled_partitions`).** The spec's rule is that
absence of progress can only be detected by comparing offsets at more than
one point in time. The MVP has no persistence and no background monitoring,
so the tool takes both samples **within a single call**: it reads the
group's committed and end offsets, waits `interval_seconds` (1–60, default
10), reads them again, and compares partition by partition:

| Status | Rule |
| --- | --- |
| `advancing` | committed offset increased |
| `caught_up` | committed offset unchanged, nothing pending |
| `not_advancing` | committed offset unchanged, messages pending |
| `moved_backward` | committed offset decreased (reset or reprocessing) |
| `unknown` | an offset is missing from either sample |

`not_advancing` partitions produce a `PARTITIONS_NOT_ADVANCING` warning. Its
evidence includes how many of them received new messages during the window.
Committed offsets are not the consumer's in-memory position: a consumer that
commits less often than the interval can look stalled. The finding says so,
and recommends sampling again with a longer interval. The call blocks for at
least `interval_seconds`, which must stay below the MCP client's request
timeout.

Alternatives considered: an in-memory snapshot store, compared across calls,
would add state and lifecycle questions. Background sampling is out of
scope for the MVP. Both remain possible later without changing the domain
function, which only compares two samples.

**Message inspection.** `inspect_messages` follows the same layering as the
rest of the server:

- `MessageReader` port: `partition_offsets()` (the topic is checked with
  DescribeTopics first, so a missing topic is never auto-created) and
  `read_range()`.
- Pure domain planning (`domain/messages/planning.py`) decides which
  offsets to read: the newest N per partition, or N from an offset.
  Retention gaps and empty partitions become explicit notes.
- Pure domain sanitization (`domain/messages/sanitizer.py`) turns raw
  messages into `InspectedMessage`s before they leave the application layer.
- `KafkaMessageAdapter` creates a short-lived consumer per call, assigns the
  planned ranges, reads up to their end (or the partition end, or the
  timeout), and closes the consumer. A timeout yields a partial result
  marked `complete: false`.

See [security.md](security.md#data-protection) for the privacy rules and
their limits.

**Controller id in KRaft.** In KRaft mode, the Metadata response's
`controller_id` is a live broker id, not the active quorum controller. The
field is documented as such in the tool's output schema.

**Missing data is `None`.** When the cluster does not report a value, such as
a cluster id, a controller or a partition leader, StreamSage returns `null`.
It never substitutes a placeholder.

**Response schemas live in presentation.** Use cases return domain entities.
The MCP layer maps them to Pydantic response models (`presentation/mcp/schemas.py`),
which define the public wire contract. A separate `application/dto` layer
would have duplicated these models without adding value in M1, so it was not
created.

**Error handling.** Infrastructure translates `KafkaException` into the
StreamSage hierarchy (`KafkaConnectionError`, `KafkaOperationTimeoutError`,
`KafkaResourceNotFoundError`, `KafkaMetadataError`, ...), and messages include
only Kafka error *codes*. `operation_boundary` turns those errors into MCP
`ToolError`/`ResourceError` with the same safe message. Any other exception
propagates to the SDK, which returns only `Error executing tool <name>`, so
internals never reach the client.

**Lifecycle.** `build_application` is a context manager. The `AdminClient` is
created once and closed when the server stops, and there are no global
singletons.

**Logging.** JSON lines go to stderr, because stdout is reserved for the stdio
transport. librdkafka logs are routed through Python logging; they are
delivered when the client is polled, which happens after every metadata
request. Each MCP operation logs `operation`, `correlation_id`, `outcome`,
`error_type` and `duration_ms`.

`correlation_id` is the JSON-RPC id of the MCP request, received through the
SDK's request `Context`. That parameter is not part of the tool's input
schema. The id is unique only within one client connection, which is enough
to match a log line to the call that produced it. The SDK does not inject a
request context into static resources, so `kafka://` resource reads log
`correlation_id: null`.
