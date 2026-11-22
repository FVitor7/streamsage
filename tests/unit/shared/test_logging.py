import io
import json
import logging

from streamsage.shared.logging import configure_logging


def test_json_lines_with_structured_fields() -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)
    logging.getLogger("streamsage.test").info(
        "tool done", extra={"event": {"operation": "list_topics", "duration_ms": 1.5}}
    )
    logging.getLogger("streamsage.test").debug("hidden")

    lines = stream.getvalue().strip().splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["level"] == "INFO"
    assert entry["message"] == "tool done"
    assert entry["operation"] == "list_topics"
    assert entry["duration_ms"] == 1.5


def test_exceptions_are_included() -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("x").exception("failed")
    assert "ValueError: boom" in json.loads(stream.getvalue())["exception"]
