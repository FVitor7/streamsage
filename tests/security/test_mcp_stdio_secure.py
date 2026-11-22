"""End-to-end: the real `streamsage` process over stdio, using SASL_SSL."""

import sys

import pytest
from mcp import Client, StdioServerParameters
from mcp.types import TextContent

from tests.fakes import random_credential
from tests.security.conftest import SASL_SSL_BOOTSTRAP, SCRAM_USER

pytestmark = pytest.mark.security


def _server(
    password: str, ca_location: str, timeout: str = "10"
) -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", "streamsage.main"],
        env={
            "STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS": SASL_SSL_BOOTSTRAP,
            "STREAMSAGE_KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "STREAMSAGE_KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
            "STREAMSAGE_KAFKA_SASL_USERNAME": SCRAM_USER,
            "STREAMSAGE_KAFKA_SASL_PASSWORD": password,
            "STREAMSAGE_KAFKA_SSL_CA_LOCATION": ca_location,
            "STREAMSAGE_KAFKA_REQUEST_TIMEOUT": timeout,
            "STREAMSAGE_KAFKA_SOCKET_TIMEOUT": timeout,
            "STREAMSAGE_LOG_LEVEL": "CRITICAL",
        },
    )


def _text(result: object) -> str:
    content = getattr(result, "content", [])
    return " ".join(c.text for c in content if isinstance(c, TextContent))


@pytest.mark.anyio
async def test_stdio_server_over_sasl_ssl(sasl_password: str, ca_location: str) -> None:
    async with Client(_server(sasl_password, ca_location)) as client:
        result = await client.call_tool("get_cluster_info", {})
    assert result.is_error is False
    assert result.structured_content is not None
    assert result.structured_content["cluster_id"] == "streamsage-local-secure-01"


@pytest.mark.anyio
async def test_stdio_server_wrong_password_is_explained_safely(
    ca_location: str,
) -> None:
    wrong_password = random_credential()
    async with Client(_server(wrong_password, ca_location, timeout="3")) as client:
        result = await client.call_tool("get_cluster_info", {})
    assert result.is_error is True
    text = _text(result)
    assert "Authentication with the Kafka cluster failed" in text
    assert wrong_password not in text
    assert "Traceback" not in text
