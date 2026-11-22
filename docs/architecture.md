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
  → KafkaMetadataSource.fetch         (AdminClient.list_topics(timeout=…))
  ← domain Topic → TopicDetailResponse (structured output)
```

## Decisions

**MCP SDK 2.x (`MCPServer`).** In `mcp` 2.x, the v1 `FastMCP` class is renamed
to `mcp.server.mcpserver.MCPServer`. We use its decorators (`tool`,
`resource`, `prompt`) and run it with `run("stdio")`. Tool handlers are plain
synchronous functions; the SDK runs them in a worker thread. As the spec
requires, this keeps the synchronous Kafka client out of asyncio code.

**One metadata call, all topics.** Both adapters use
`AdminClient.list_topics(timeout=…)` with no topic argument. That single
response contains the cluster id, the controller id, the brokers and every
topic's partitions, leaders, replicas and ISR. `describe_topic` finds the
requested topic in this full listing. We checked against Kafka 4.3: a
metadata request scoped to a missing topic name (`list_topics(topic=…)`)
*did* auto-create that topic on a broker with
`auto.create.topics.enable=true`. The local Docker broker keeps auto-creation
enabled on purpose, and
`test_missing_topic_raises_and_is_never_created` guards this behavior. The
trade-off is that `describe_topic` fetches metadata for every topic. That is
fine for M1 and can be revisited for very large clusters.

**Internal topics.** Cluster metadata does not carry an "internal" flag.
Topics whose names start with `__`, Kafka's reserved prefix, are treated as
internal (for example `__consumer_offsets` and `__transaction_state`).

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
