"""Error boundary between use cases and the MCP protocol.

Every tool/resource call goes through :func:`operation_boundary`, which logs
the operation name, duration and outcome, and turns anticipated StreamSage
errors into MCP errors carrying a safe message. Unexpected exceptions are
re-raised untouched: the SDK then logs the traceback server-side and returns
only a generic message to the client, so internals never leak.
"""

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager

from streamsage.domain.exceptions import StreamSageError

logger = logging.getLogger("streamsage.mcp")


@contextmanager
def operation_boundary(operation: str, error_type: type[Exception]) -> Iterator[None]:
    """Instrument one MCP operation and normalize its errors.

    ``error_type`` is the SDK exception to raise for anticipated failures
    (``ToolError`` for tools, ``ResourceError`` for resources).
    """
    started = time.perf_counter()
    try:
        yield
    except StreamSageError as exc:
        _log(operation, started, outcome="error", error_type=type(exc).__name__)
        raise error_type(str(exc)) from None
    except Exception as exc:
        _log(operation, started, outcome="crash", error_type=type(exc).__name__)
        raise
    _log(operation, started, outcome="success", error_type=None)


def _log(
    operation: str, started: float, *, outcome: str, error_type: str | None
) -> None:
    logger.info(
        "mcp operation finished",
        extra={
            "event": {
                "operation": operation,
                "outcome": outcome,
                "error_type": error_type,
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            }
        },
    )
