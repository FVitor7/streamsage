"""MCP-level tests: real MCPServer, in-process client, fake readers."""

import io
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from mcp import Client
from mcp.shared.exceptions import MCPError
from mcp.types import TextContent

from streamsage.application.use_cases.describe_consumer_group import (
    DescribeConsumerGroupUseCase,
)
from streamsage.application.use_cases.describe_topic import DescribeTopicUseCase
from streamsage.application.use_cases.get_cluster_info import GetClusterInfoUseCase
from streamsage.application.use_cases.get_consumer_lag import GetConsumerLagUseCase
from streamsage.application.use_cases.list_consumer_groups import (
    ListConsumerGroupsUseCase,
)
from streamsage.application.use_cases.list_topics import ListTopicsUseCase
from streamsage.domain.exceptions import (
    KafkaConnectionError,
    KafkaOperationTimeoutError,
)
from streamsage.presentation.mcp.server import create_server
from streamsage.shared.logging import configure_logging
from tests.fakes import (
    FakeClusterReader,
    FakeConsumerGroupReader,
    FakeTopicReader,
    make_topic,
    random_credential,
)

TOPICS = [
    make_topic("orders.created", 3, replicas=(1, 2), isr=(1,)),
    make_topic("payments", 1),
    make_topic("__consumer_offsets", 2),
]


def build(
    cluster_reader: FakeClusterReader | None = None,
    topic_reader: FakeTopicReader | None = None,
    include_internal_topics: bool = False,
    group_reader: FakeConsumerGroupReader | None = None,
) -> Any:
    topic_reader = topic_reader or FakeTopicReader(TOPICS)
    group_reader = group_reader or FakeConsumerGroupReader()
    return create_server(
        get_cluster_info_use_case=GetClusterInfoUseCase(
            cluster_reader or FakeClusterReader()
        ),
        list_topics_use_case=ListTopicsUseCase(topic_reader),
        describe_topic_use_case=DescribeTopicUseCase(topic_reader),
        list_consumer_groups_use_case=ListConsumerGroupsUseCase(group_reader),
        describe_consumer_group_use_case=DescribeConsumerGroupUseCase(group_reader),
        get_consumer_lag_use_case=GetConsumerLagUseCase(group_reader),
        include_internal_topics=include_internal_topics,
    )


@pytest.fixture
async def client() -> AsyncIterator[Client]:
    async with Client(build()) as c:
        yield c


def _text(result: Any) -> str:
    return " ".join(c.text for c in result.content if isinstance(c, TextContent))


@pytest.mark.anyio
async def test_registers_exactly_the_expected_tools_as_read_only(
    client: Client,
) -> None:
    tools = {tool.name: tool for tool in (await client.list_tools()).tools}
    assert set(tools) == {
        "get_cluster_info",
        "list_topics",
        "describe_topic",
        "list_consumer_groups",
        "describe_consumer_group",
        "get_consumer_lag",
    }
    for tool in tools.values():
        assert tool.description
        assert tool.output_schema is not None
        assert tool.annotations is not None
        assert tool.annotations.read_only_hint is True
        assert tool.annotations.destructive_hint is False


@pytest.mark.anyio
async def test_describe_topic_schema_constrains_topic_name(client: Client) -> None:
    tools = {tool.name: tool for tool in (await client.list_tools()).tools}
    prop = tools["describe_topic"].input_schema["properties"]["topic_name"]
    assert prop["maxLength"] == 249
    assert prop["pattern"] == "^[a-zA-Z0-9._-]+$"
    assert tools["describe_topic"].input_schema["required"] == ["topic_name"]


@pytest.mark.anyio
async def test_get_cluster_info(client: Client) -> None:
    result = await client.call_tool("get_cluster_info", {})
    assert result.is_error is False
    assert result.structured_content == {
        "cluster_id": "test-cluster",
        "controller_id": 1,
        "broker_count": 2,
        "brokers": [
            {"broker_id": 1, "host": "broker-1", "port": 9092},
            {"broker_id": 2, "host": "broker-2", "port": 9092},
        ],
    }


@pytest.mark.anyio
async def test_list_topics_hides_internal_by_default(client: Client) -> None:
    result = await client.call_tool("list_topics", {})
    assert result.structured_content is not None
    assert result.structured_content["include_internal"] is False
    names = [t["name"] for t in result.structured_content["topics"]]
    assert names == ["orders.created", "payments"]
    assert result.structured_content["topics"][0]["partition_count"] == 3
    assert result.structured_content["topics"][0]["replication_factor"] == 2


@pytest.mark.anyio
async def test_list_topics_include_internal(client: Client) -> None:
    result = await client.call_tool("list_topics", {"include_internal": True})
    assert result.structured_content is not None
    names = [t["name"] for t in result.structured_content["topics"]]
    assert names == ["__consumer_offsets", "orders.created", "payments"]


@pytest.mark.anyio
async def test_internal_topics_default_follows_server_setting() -> None:
    async with Client(build(include_internal_topics=True)) as c:
        result = await c.call_tool("list_topics", {})
    assert result.structured_content is not None
    assert result.structured_content["topic_count"] == 3


@pytest.mark.anyio
async def test_describe_topic(client: Client) -> None:
    result = await client.call_tool("describe_topic", {"topic_name": "orders.created"})
    assert result.is_error is False
    data = result.structured_content
    assert data is not None
    assert data["partition_count"] == 3
    assert data["under_replicated_partition_count"] == 3
    assert data["leaderless_partition_count"] == 0
    assert data["partitions"][0] == {
        "partition_id": 0,
        "leader": 1,
        "replicas": [1, 2],
        "isr": [1],
        "under_replicated": True,
        "metadata_error": None,
    }


@pytest.mark.anyio
async def test_describe_missing_topic_is_controlled_error(client: Client) -> None:
    result = await client.call_tool("describe_topic", {"topic_name": "missing"})
    assert result.is_error is True
    assert "Topic 'missing' does not exist." in _text(result)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "arguments",
    [{}, {"topic_name": ""}, {"topic_name": "bad name"}, {"topic_name": "x" * 250}],
)
async def test_describe_topic_rejects_invalid_arguments(
    arguments: dict[str, Any],
) -> None:
    reader = FakeTopicReader(TOPICS)
    async with Client(build(topic_reader=reader)) as c:
        result = await c.call_tool("describe_topic", arguments)
    assert result.is_error is True
    assert reader.described == []


@pytest.mark.anyio
async def test_list_topics_rejects_wrong_argument_type(client: Client) -> None:
    result = await client.call_tool("list_topics", {"include_internal": "maybe"})
    assert result.is_error is True


@pytest.mark.anyio
@pytest.mark.parametrize(
    "error",
    [
        KafkaConnectionError("Could not connect to the Kafka cluster."),
        KafkaOperationTimeoutError("Kafka cluster metadata request timed out."),
    ],
)
async def test_known_errors_return_safe_message(error: Exception) -> None:
    async with Client(build(cluster_reader=FakeClusterReader(error=error))) as c:
        result = await c.call_tool("get_cluster_info", {})
    assert result.is_error is True
    assert str(error) in _text(result)
    assert "Traceback" not in _text(result)


@pytest.mark.anyio
async def test_unexpected_errors_do_not_leak_details() -> None:
    sensitive = random_credential()
    error = RuntimeError(f"internal detail {sensitive} at /internal/path.py line 42")
    async with Client(build(cluster_reader=FakeClusterReader(error=error))) as c:
        result = await c.call_tool("get_cluster_info", {})
    assert result.is_error is True
    text = _text(result)
    assert sensitive not in text
    assert "/internal/path.py" not in text
    assert "Traceback" not in text


@pytest.mark.anyio
async def test_resources(client: Client) -> None:
    uris = {str(r.uri) for r in (await client.list_resources()).resources}
    assert uris == {"kafka://cluster/info", "kafka://topics"}

    info = await client.read_resource("kafka://cluster/info")
    assert json.loads(info.contents[0].text)["cluster_id"] == "test-cluster"  # type: ignore[union-attr]

    topics = await client.read_resource("kafka://topics")
    data = json.loads(topics.contents[0].text)  # type: ignore[union-attr]
    assert data["include_internal"] is False
    assert [t["name"] for t in data["topics"]] == ["orders.created", "payments"]


@pytest.mark.anyio
async def test_resource_errors_are_controlled() -> None:
    reader = FakeClusterReader(error=KafkaConnectionError("Could not connect."))
    async with Client(build(cluster_reader=reader)) as c:
        with pytest.raises(MCPError, match="Could not connect."):
            await c.read_resource("kafka://cluster/info")


@pytest.mark.anyio
async def test_investigation_prompt(client: Client) -> None:
    prompts = {p.name: p for p in (await client.list_prompts()).prompts}
    assert set(prompts) == {"investigate_kafka_cluster"}

    plain = await client.get_prompt("investigate_kafka_cluster", {})
    text = plain.messages[0].content.text  # type: ignore[union-attr]
    for expected in ("get_cluster_info", "list_topics", "describe_topic"):
        assert expected in text
    assert "Hypotheses" in text
    assert "Do not claim the cluster is fully healthy" in text

    focused = await client.get_prompt(
        "investigate_kafka_cluster", {"focus_topic": "orders.created"}
    )
    assert "`orders.created`" in focused.messages[0].content.text  # type: ignore[union-attr]


@pytest.mark.anyio
async def test_request_context_is_not_exposed_as_a_tool_argument(
    client: Client,
) -> None:
    for tool in (await client.list_tools()).tools:
        assert "ctx" not in tool.input_schema.get("properties", {})
        assert "ctx" not in tool.input_schema.get("required", [])


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("tool", "arguments"),
    [
        ("get_cluster_info", {}),
        ("list_topics", {}),
        ("describe_topic", {"topic_name": "orders.created"}),
        ("describe_topic", {"topic_name": "missing"}),
    ],
)
async def test_tool_calls_log_a_correlation_id(
    client: Client, tool: str, arguments: dict[str, Any]
) -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)
    await client.call_tool(tool, arguments)

    entries = [
        entry
        for entry in map(json.loads, stream.getvalue().splitlines())
        if entry.get("message") == "mcp operation finished"
    ]
    assert [entry["operation"] for entry in entries] == [tool]
    correlation_id = entries[0]["correlation_id"]
    assert isinstance(correlation_id, str) and correlation_id
