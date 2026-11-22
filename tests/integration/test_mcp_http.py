"""End-to-end: the real `streamsage` process serving Streamable HTTP."""

import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator

import httpx2
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from streamsage.presentation.http.bearer_auth import generate_token, hash_token
from tests.integration.conftest import BOOTSTRAP

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("admin_client")]

READER_TOKEN = generate_token()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="module")
def http_url() -> Iterator[str]:
    port = _free_port()
    env = {
        **os.environ,
        "STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP,
        "STREAMSAGE_TRANSPORT": "http",
        "STREAMSAGE_HTTP_PORT": str(port),
        "STREAMSAGE_HTTP_TOKENS": json.dumps(
            [
                {
                    "name": "it-reader",
                    "sha256": hash_token(READER_TOKEN),
                    "scopes": ["kafka:metadata"],
                }
            ]
        ),
        "STREAMSAGE_LOG_LEVEL": "WARNING",
    }
    process = subprocess.Popen(
        [sys.executable, "-m", "streamsage.main"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    url = f"http://127.0.0.1:{port}/mcp"
    deadline = time.monotonic() + 20
    while True:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            if process.poll() is not None or time.monotonic() > deadline:
                stderr = process.stderr.read().decode() if process.stderr else ""
                process.kill()
                pytest.fail(f"streamsage did not start: {stderr[-2000:]}")
            time.sleep(0.2)
    yield url
    process.terminate()
    process.wait(timeout=10)


def test_unauthenticated_request_is_rejected(http_url: str) -> None:
    response = httpx2.post(http_url, json={"jsonrpc": "2.0", "id": 1, "method": "ping"})
    assert response.status_code == 401


@pytest.mark.anyio
async def test_reader_token_queries_real_kafka_but_cannot_read_messages(
    http_url: str,
) -> None:
    async with (
        httpx2.AsyncClient(headers={"Authorization": f"Bearer {READER_TOKEN}"}) as http,
        Client(streamable_http_client(http_url, http_client=http)) as client,
    ):
        tools = {t.name for t in (await client.list_tools()).tools}
        assert "get_consumer_lag" in tools
        assert "inspect_messages" not in tools

        lag = await client.call_tool("get_consumer_lag", {"group_id": "orders-service"})
        assert lag.structured_content is not None
        assert lag.structured_content["total_lag"] == 180

        denied = await client.call_tool(
            "inspect_messages", {"topic_name": "customers.updated"}
        )
        assert denied.is_error is True
