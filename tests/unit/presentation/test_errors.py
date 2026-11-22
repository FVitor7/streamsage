import io
import json

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from streamsage.domain.exceptions import KafkaConnectionError
from streamsage.presentation.mcp.errors import operation_boundary
from streamsage.shared.logging import configure_logging


@pytest.fixture
def log_stream() -> io.StringIO:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)
    return stream


def _entries(stream: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_success_is_logged(log_stream: io.StringIO) -> None:
    with operation_boundary("op", ToolError):
        pass
    (entry,) = _entries(log_stream)
    assert entry["operation"] == "op"
    assert entry["outcome"] == "success"
    assert entry["error_type"] is None
    assert isinstance(entry["duration_ms"], float)


def test_known_error_is_converted(log_stream: io.StringIO) -> None:
    with (
        pytest.raises(ToolError, match="cannot connect") as raised,
        operation_boundary("op", ToolError),
    ):
        raise KafkaConnectionError("cannot connect")
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
    (entry,) = _entries(log_stream)
    assert entry["outcome"] == "error"
    assert entry["error_type"] == "KafkaConnectionError"


def test_unexpected_error_is_reraised(log_stream: io.StringIO) -> None:
    with pytest.raises(RuntimeError), operation_boundary("op", ToolError):
        raise RuntimeError("bug")
    (entry,) = _entries(log_stream)
    assert entry["outcome"] == "crash"
    assert entry["error_type"] == "RuntimeError"
