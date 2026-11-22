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
from tests.unit.presentation.test_mcp_server import build

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
    app = build_http_app(build(), settings)
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
