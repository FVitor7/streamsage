"""MCP tests for the consumer group tools (in-process client, fakes)."""

from collections.abc import AsyncIterator
from typing import Any

import pytest
from mcp import Client
from mcp.types import TextContent

from streamsage.domain.exceptions import KafkaConnectionError
from streamsage.domain.value_objects.lag import PartitionLag
from tests.fakes import FakeConsumerGroupReader
from tests.unit.presentation.test_mcp_server import build


@pytest.fixture
async def client() -> AsyncIterator[Client]:
    async with Client(build()) as c:
        yield c


def _text(result: Any) -> str:
    return " ".join(c.text for c in result.content if isinstance(c, TextContent))


@pytest.mark.anyio
async def test_list_consumer_groups(client: Client) -> None:
    result = await client.call_tool("list_consumer_groups", {})
    assert result.is_error is False
    assert result.structured_content == {
        "group_count": 3,
        "groups": [
            {"group_id": "billing", "state": "stable", "is_simple": False},
            {"group_id": "legacy", "state": "dead", "is_simple": False},
            {"group_id": "orders-service", "state": "empty", "is_simple": True},
        ],
    }


@pytest.mark.anyio
async def test_list_consumer_groups_state_filter(client: Client) -> None:
    result = await client.call_tool(
        "list_consumer_groups", {"states": ["stable", "empty"]}
    )
    assert result.structured_content is not None
    assert [g["group_id"] for g in result.structured_content["groups"]] == [
        "billing",
        "orders-service",
    ]


@pytest.mark.anyio
async def test_list_consumer_groups_rejects_unknown_state(client: Client) -> None:
    result = await client.call_tool("list_consumer_groups", {"states": ["sleepy"]})
    assert result.is_error is True


@pytest.mark.anyio
async def test_describe_consumer_group(client: Client) -> None:
    result = await client.call_tool("describe_consumer_group", {"group_id": "billing"})
    assert result.is_error is False
    data = result.structured_content
    assert data is not None
    assert data["state"] == "stable"
    assert data["coordinator_id"] == 1
    assert data["member_count"] == 1
    assert data["members"][0] == {
        "member_id": "m-1",
        "client_id": "billing-1",
        "host": "/10.0.0.5",
        "group_instance_id": None,
        "assignment": [
            {"topic": "payments", "partition": 1},
            {"topic": "payments", "partition": 0},
        ],
    }


@pytest.mark.anyio
async def test_describe_missing_group_is_controlled_error(client: Client) -> None:
    result = await client.call_tool("describe_consumer_group", {"group_id": "ghost"})
    assert result.is_error is True
    assert "Consumer group 'ghost' does not exist." in _text(result)


@pytest.mark.anyio
async def test_get_consumer_lag(client: Client) -> None:
    result = await client.call_tool("get_consumer_lag", {"group_id": "billing"})
    assert result.is_error is False
    assert result.structured_content == {
        "group_id": "billing",
        "total_lag": 70,
        "max_lag": 60,
        "partition_count": 3,
        "unknown_lag_partition_count": 1,
        "truncated": False,
        "partitions": [
            {
                "topic": "payments",
                "partition": 0,
                "committed_offset": 40,
                "log_end_offset": 100,
                "lag": 60,
            },
            {
                "topic": "payments",
                "partition": 2,
                "committed_offset": 90,
                "log_end_offset": 100,
                "lag": 10,
            },
            {
                "topic": "payments",
                "partition": 1,
                "committed_offset": None,
                "log_end_offset": 80,
                "lag": None,
            },
        ],
    }


@pytest.mark.anyio
async def test_get_consumer_lag_truncates_but_keeps_totals() -> None:
    lag = [PartitionLag("t", p, 0, 10 * (p + 1)) for p in range(5)]
    async with Client(build(group_reader=FakeConsumerGroupReader(lag=lag))) as c:
        result = await c.call_tool(
            "get_consumer_lag", {"group_id": "billing", "max_partitions": 2}
        )
    data = result.structured_content
    assert data is not None
    assert data["truncated"] is True
    assert data["partition_count"] == 5
    assert data["total_lag"] == 150
    assert [p["partition"] for p in data["partitions"]] == [4, 3]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"group_id": ""},
        {"group_id": "x" * 256},
        {"group_id": "billing", "max_partitions": 0},
        {"group_id": "billing", "max_partitions": 1001},
    ],
)
async def test_get_consumer_lag_rejects_invalid_arguments(
    arguments: dict[str, Any],
) -> None:
    reader = FakeConsumerGroupReader()
    async with Client(build(group_reader=reader)) as c:
        result = await c.call_tool("get_consumer_lag", arguments)
    assert result.is_error is True
    assert reader.requested == []


@pytest.mark.anyio
async def test_consumer_tool_errors_are_safe() -> None:
    reader = FakeConsumerGroupReader(error=KafkaConnectionError("Could not connect."))
    async with Client(build(group_reader=reader)) as c:
        result = await c.call_tool("list_consumer_groups", {})
    assert result.is_error is True
    assert "Could not connect." in _text(result)
    assert "Traceback" not in _text(result)


@pytest.mark.anyio
async def test_lag_tool_description_warns_about_snapshots(client: Client) -> None:
    tools = {t.name: t for t in (await client.list_tools()).tools}
    description = tools["get_consumer_lag"].description or ""
    assert "never 0" in description
    assert "does not prove a consumer is stuck" in description
