"""Structured logging for the StreamSage process.

Logs are written as JSON lines to **stderr**: with the stdio transport,
stdout carries MCP protocol messages and must not receive anything else.

Structured fields are passed as ``extra={"event": {...}}``. Callers must not
put credentials or message payloads into log records.
"""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import TextIO

EVENT_ATTR = "event"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        event = getattr(record, EVENT_ATTR, None)
        if isinstance(event, dict):
            entry.update(event)
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(level: str, stream: TextIO | None = None) -> None:
    """Install the JSON handler on the root logger.

    Must run before the MCP server is created: the SDK only installs its own
    handler when the root logger has none.
    """
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
