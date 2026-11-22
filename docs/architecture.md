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

**Stalled partitions are an M3 feature.** The spec's M2 tool table lists
`detect_stalled_partitions`, but proving that a consumer is not progressing
requires comparing offsets at more than one point in time (spec, M3), and M2
has no persistence or background sampling. Lag is a single snapshot, and high
lag alone does not prove a stall. The tool is therefore deferred to M3, where
offset snapshots are introduced.

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
