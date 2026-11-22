import pytest


@pytest.fixture
def anyio_backend() -> str:
    """Run async tests (anyio pytest plugin) on asyncio, like the MCP server."""
    return "asyncio"
