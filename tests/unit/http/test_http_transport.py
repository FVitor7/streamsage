"""End-to-end over real HTTP: uvicorn + bearer auth + scopes (no Kafka)."""

import socket
import threading
import time
from collections.abc import Iterator

import httpx2
import pytest
import uvicorn
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import TextContent

from streamsage.infrastructure.config.settings import HttpTokenSetting, Settings
from streamsage.presentation.http.app import MCP_PATH, build_http_app
from streamsage.presentation.http.bearer_auth import generate_token, hash_token
from streamsage.shared.metrics import ServerMetrics
from tests.unit.presentation.test_mcp_server import build

METRICS = ServerMetrics("test")
METADATA_TOKEN = generate_token()
FULL_TOKEN = generate_token()
MESSAGE_TOOLS = {"inspect_messages", "find_messages_by_key"}


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="module")
def base_url() -> Iterator[str]:
    port = _free_port()
    settings = Settings(
        transport="http",
        http_port=port,
        http_tokens=[
            HttpTokenSetting(
                name="reader",
                sha256=hash_token(METADATA_TOKEN),
                scopes=["kafka:metadata"],
            ),
            HttpTokenSetting(
                name="admin",
                sha256=hash_token(FULL_TOKEN),
                scopes=["kafka:metadata", "kafka:messages"],
            ),
        ],
    )
    app = build_http_app(build(metrics=METRICS), settings, METRICS)
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_config=None, lifespan="on")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert time.monotonic() < deadline, "uvicorn did not start"
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


def _client(base_url: str, token: str) -> tuple[httpx2.AsyncClient, Client]:
    http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {token}"})
    return http, Client(streamable_http_client(base_url + MCP_PATH, http_client=http))


def test_requests_without_valid_token_get_401(base_url: str) -> None:
    body = {"jsonrpc": "2.0", "id": 1, "method": "ping"}
    for headers in ({}, {"Authorization": "Bearer nope"}):
        response = httpx2.post(base_url + MCP_PATH, json=body, headers=headers)
        assert response.status_code == 401
        assert response.headers["www-authenticate"].startswith("Bearer")


@pytest.mark.anyio
async def test_metadata_token_cannot_see_or_call_message_tools(base_url: str) -> None:
    http, client = _client(base_url, METADATA_TOKEN)
    async with http, client:
        tools = {t.name for t in (await client.list_tools()).tools}
        assert "get_cluster_info" in tools
        assert not tools & MESSAGE_TOOLS

        allowed = await client.call_tool("get_cluster_info", {})
        assert allowed.is_error is False

        denied = await client.call_tool("inspect_messages", {"topic_name": "orders"})
        assert denied.is_error is True
        text = " ".join(c.text for c in denied.content if isinstance(c, TextContent))
        assert "kafka:messages" in text


@pytest.mark.anyio
async def test_full_token_can_use_every_tool(base_url: str) -> None:
    http, client = _client(base_url, FULL_TOKEN)
    async with http, client:
        tools = {t.name for t in (await client.list_tools()).tools}
        assert tools >= MESSAGE_TOOLS
        result = await client.call_tool("inspect_messages", {"topic_name": "orders"})
        assert result.is_error is False
        resource = await client.read_resource("kafka://cluster/info")
        assert resource.contents


def test_healthz_is_public(base_url: str) -> None:
    response = httpx2.get(base_url + "/healthz")
    assert (response.status_code, response.text) == (200, "ok")


@pytest.mark.anyio
async def test_metrics_require_a_token_and_reflect_tool_calls(base_url: str) -> None:
    assert httpx2.get(base_url + "/metrics").status_code == 401

    http, client = _client(base_url, METADATA_TOKEN)
    async with http, client:
        await client.call_tool("list_topics", {})
        await client.call_tool("inspect_messages", {"topic_name": "orders"})  # denied

    response = httpx2.get(
        base_url + "/metrics", headers={"Authorization": f"Bearer {METADATA_TOKEN}"}
    )
    assert response.status_code == 200
    body = response.text
    assert 'streamsage_tool_calls_total{outcome="success",tool="list_topics"}' in body
    assert (
        'streamsage_tool_calls_total{outcome="error",tool="inspect_messages"}' in body
    )
    assert 'streamsage_authorization_denials_total{target="inspect_messages"}' in body
    assert 'streamsage_http_auth_failures_total{reason="missing_token"}' in body
    assert 'streamsage_build_info{version="test"} 1.0' in body


@pytest.mark.anyio
@pytest.mark.parametrize(("public", "status"), [(True, 200), (False, 401)])
async def test_metrics_public_setting(public: bool, status: int) -> None:
    settings = Settings(
        transport="http",
        http_metrics_public=public,
        http_tokens=[
            HttpTokenSetting(
                name="x", sha256=hash_token(generate_token()), scopes=["kafka:metadata"]
            )
        ],
    )
    app = build_http_app(build(), settings, ServerMetrics("t"))
    sent: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/metrics",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 1),
    }
    await app(scope, receive, send)  # type: ignore[arg-type]
    assert sent[0]["status"] == status
