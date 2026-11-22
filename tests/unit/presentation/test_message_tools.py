from collections.abc import AsyncIterator
from typing import Any

import pytest
from mcp import Client
from mcp.types import TextContent

from streamsage.domain.entities.message import RawHeader, RawMessage, TimestampType
from streamsage.domain.exceptions import KafkaConnectionError
from tests.fakes import FakeMessageReader, random_credential
from tests.unit.presentation.test_mcp_server import build

AUTHORIZATION = random_credential().encode()

SENSITIVE = RawMessage(
    topic="customers",
    partition=0,
    offset=3,
    timestamp_ms=1_700_000_000_000,
    timestamp_type=TimestampType.CREATE_TIME,
    key=b"cust-1",
    value=b'{"id":1,"email":"ana@example.com","city":"Recife"}',
    headers=(RawHeader("Authorization", AUTHORIZATION), RawHeader("trace", b"t-1")),
)


def _reader() -> FakeMessageReader:
    from streamsage.domain.entities.message import PartitionOffsets

    return FakeMessageReader(offsets=[PartitionOffsets(0, 0, 4)], messages=[SENSITIVE])


@pytest.fixture
async def client() -> AsyncIterator[Client]:
    async with Client(build(message_reader=_reader())) as c:
        yield c


def _text(result: Any) -> str:
    return " ".join(c.text for c in result.content if isinstance(c, TextContent))


@pytest.mark.anyio
async def test_inspect_messages_without_payloads(client: Client) -> None:
    result = await client.call_tool("inspect_messages", {"topic_name": "customers"})
    assert result.is_error is False
    data = result.structured_content
    assert data is not None
    assert data["payloads_included"] is False
    assert data["message_count"] == 1
    message = data["messages"][0]
    assert message["timestamp"] == "2023-11-14T22:13:20+00:00"
    assert message["key"] == "cust-1"
    assert message["payload_treatment"] == "omitted"
    assert message["payload_preview"] is None
    assert message["payload_size"] == len(SENSITIVE.value or b"")
    assert message["headers"][0] == {
        "key": "Authorization",
        "value": "[REDACTED]",
        "value_size": len(AUTHORIZATION),
        "encoding": "text",
        "redacted": True,
        "truncated": False,
    }
    assert "ana@example.com" not in str(data)
    assert AUTHORIZATION.decode() not in str(data)


@pytest.mark.anyio
async def test_inspect_messages_with_payloads_redacts_fields() -> None:
    async with Client(build(message_reader=_reader(), payloads_enabled=True)) as c:
        result = await c.call_tool("inspect_messages", {"topic_name": "customers"})
    data = result.structured_content
    assert data is not None
    message = data["messages"][0]
    assert message["payload_treatment"] == "json_redacted"
    assert message["payload_preview"] == (
        '{"id":1,"email":"[REDACTED]","city":"Recife"}'
    )
    assert message["redacted_fields"] == ["email"]
    assert "ana@example.com" not in str(data)


@pytest.mark.anyio
async def test_tool_description_states_the_payload_policy() -> None:
    async with Client(build(payloads_enabled=False)) as off:
        tools = {t.name: t for t in (await off.list_tools()).tools}
        assert "Payloads are omitted" in (tools["inspect_messages"].description or "")
    async with Client(build(payloads_enabled=True)) as on:
        tools = {t.name: t for t in (await on.list_tools()).tools}
        assert "redacted" in (tools["inspect_messages"].description or "")


@pytest.mark.anyio
@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"topic_name": "bad name"},
        {"topic_name": "t", "limit": 0},
        {"topic_name": "t", "limit": 51},
        {"topic_name": "t", "partition": -1},
        {"topic_name": "t", "start_offset": -1, "partition": 0},
        {"topic_name": "t", "start_offset": 1},  # needs a partition
    ],
)
async def test_invalid_arguments(client: Client, arguments: dict[str, Any]) -> None:
    result = await client.call_tool("inspect_messages", arguments)
    assert result.is_error is True


@pytest.mark.anyio
async def test_missing_topic_and_errors_are_controlled(client: Client) -> None:
    result = await client.call_tool("inspect_messages", {"topic_name": "missing"})
    assert result.is_error is True
    assert "Topic 'missing' does not exist." in _text(result)

    reader = FakeMessageReader(error=KafkaConnectionError("Could not connect."))
    async with Client(build(message_reader=reader)) as c:
        result = await c.call_tool("inspect_messages", {"topic_name": "orders"})
    assert result.is_error is True
    assert "Could not connect." in _text(result)
