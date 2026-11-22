"""Error boundary between use cases and the MCP protocol.

Every tool/resource call goes through :func:`operation_boundary`, which logs
the operation name, correlation id, duration and outcome, and turns
anticipated StreamSage errors into MCP errors carrying a safe message.
Unexpected exceptions are re-raised untouched: the SDK then logs the
traceback server-side and returns only a generic message to the client, so
internals never leak.
"""

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager

from streamsage.domain.exceptions import StreamSageError

logger = logging.getLogger("streamsage.mcp")


@contextmanager
def operation_boundary(
    operation: str,
    error_type: type[Exception],
    correlation_id: str | None = None,
) -> Iterator[None]:
    """Instrument one MCP operation and normalize its errors.

    ``error_type`` is the SDK exception to raise for anticipated failures
    (``ToolError`` for tools, ``ResourceError`` for resources).
    ``correlation_id`` is the MCP request id when available, so server logs
    can be matched to a specific client call. The SDK does not inject a
    request context into static resources, so resource logs have none.
    """
    started = time.perf_counter()

    def log(outcome: str, error: str | None) -> None:
        logger.info(
            "mcp operation finished",
            extra={
                "event": {
                    "operation": operation,
                    "correlation_id": correlation_id,
                    "outcome": outcome,
                    "error_type": error,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                }
            },
        )

    try:
        yield
    except StreamSageError as exc:
        log("error", type(exc).__name__)
        raise error_type(str(exc)) from None
    except Exception as exc:
        log("crash", type(exc).__name__)
        raise
    log("success", None)
