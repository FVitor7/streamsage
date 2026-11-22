"""End-to-end: launch `streamsage` over stdio and call tools against real Kafka."""

import sys

import pytest
from mcp import Client, StdioServerParameters
from mcp.types import TextContent

from tests.integration.conftest import BOOTSTRAP, SAMPLE_TOPICS

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("admin_client")]


def _server(**env: str) -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", "streamsage.main"],
        env={
            "STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP,
            "STREAMSAGE_LOG_LEVEL": "WARNING",
            **env,
        },
    )


@pytest.mark.anyio
async def test_stdio_server_serves_tools_against_real_kafka() -> None:
    async with Client(_server()) as client:
        tools = {tool.name for tool in (await client.list_tools()).tools}
        assert {"get_cluster_info", "list_topics", "describe_topic"} <= tools
        assert {"list_consumer_groups", "get_consumer_lag"} <= tools

        info = await client.call_tool("get_cluster_info", {})
        assert info.is_error is False
        assert info.structured_content is not None
        assert info.structured_content["broker_count"] >= 1

        listed = await client.call_tool("list_topics", {})
        assert listed.structured_content is not None
        names = {t["name"] for t in listed.structured_content["topics"]}
        assert set(SAMPLE_TOPICS) <= names
        assert "__consumer_offsets" not in names

        described = await client.call_tool(
            "describe_topic", {"topic_name": "orders.created"}
        )
        assert described.structured_content is not None
        assert described.structured_content["partition_count"] == 3

        resource = await client.read_resource("kafka://cluster/info")
        assert resource.contents

        lag = await client.call_tool("get_consumer_lag", {"group_id": "orders-service"})
        assert lag.is_error is False
        assert lag.structured_content is not None
        assert lag.structured_content["total_lag"] == 180


@pytest.mark.anyio
async def test_stdio_server_reports_unreachable_cluster_safely() -> None:
    params = _server(
        STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:1",
        STREAMSAGE_KAFKA_REQUEST_TIMEOUT="2",
        STREAMSAGE_KAFKA_SOCKET_TIMEOUT="2",
        STREAMSAGE_LOG_LEVEL="CRITICAL",
    )
    async with Client(params) as client:
        result = await client.call_tool("get_cluster_info", {})
    assert result.is_error is True
    text = " ".join(c.text for c in result.content if isinstance(c, TextContent))
    assert "Could not connect to the Kafka cluster" in text
    assert "Traceback" not in text


@pytest.mark.anyio
async def test_stdio_server_applies_configured_lag_thresholds() -> None:
    # orders-service has a max partition lag of ~66 messages (seeded data).
    params = _server(
        STREAMSAGE_LAG_WARNING_THRESHOLD="10",
        STREAMSAGE_LAG_CRITICAL_THRESHOLD="60",
    )
    async with Client(params) as client:
        result = await client.call_tool(
            "analyze_consumer_health", {"group_id": "orders-service"}
        )
    assert result.is_error is False
    data = result.structured_content
    assert data is not None
    assert data["status"] == "critical"
    assert data["thresholds"] == {"warning": 10, "critical": 60}
    assert [f["code"] for f in data["findings"]] == [
        "LAG_ABOVE_CRITICAL",
        "NO_ACTIVE_MEMBERS",
    ]


@pytest.mark.anyio
async def test_stdio_server_detects_partitions_not_advancing() -> None:
    async with Client(_server()) as client:
        result = await client.call_tool(
            "detect_stalled_partitions",
            {"group_id": "orders-service", "interval_seconds": 1},
        )
    assert result.is_error is False
    data = result.structured_content
    assert data is not None
    assert data["partition_counts"]["not_advancing"] == 3
    assert data["status"] == "warning"


@pytest.mark.anyio
@pytest.mark.parametrize("payloads", [False, True])
async def test_stdio_server_inspects_messages_safely(
    payloads: bool, customer_secrets: set[str]
) -> None:
    params = _server(STREAMSAGE_MESSAGE_PAYLOADS_ENABLED=str(payloads).lower())
    async with Client(params) as client:
        result = await client.call_tool(
            "inspect_messages", {"topic_name": "customers.updated"}
        )
    assert result.is_error is False
    data = result.structured_content
    assert data is not None
    assert data["payloads_included"] is payloads
    assert data["message_count"] == 3
    for secret in customer_secrets:
        assert secret not in str(data)


@pytest.mark.anyio
async def test_stdio_server_key_search_and_dlq() -> None:
    async with Client(_server(STREAMSAGE_DLQ_TOPICS="*.DLQ")) as client:
        found = await client.call_tool(
            "find_messages_by_key", {"topic_name": "orders.created", "key": "key-7"}
        )
        dlq = await client.call_tool("list_dlq_topics", {})
    assert found.structured_content is not None
    assert [m["key"] for m in found.structured_content["matches"]] == ["key-7"]
    assert dlq.structured_content is not None
    assert [t["name"] for t in dlq.structured_content["topics"]] == [
        "orders.created.DLQ"
    ]
