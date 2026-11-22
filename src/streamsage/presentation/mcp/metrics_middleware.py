import time
from typing import Any

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.types import CallToolResult

from streamsage.presentation.mcp.authorization import TOOL_SCOPES
from streamsage.shared.metrics import ServerMetrics

UNKNOWN_TOOL = "unknown"


class MetricsMiddleware:
    """Records duration and outcome of every ``tools/call``.

    Tool names come from the client, so names StreamSage does not define are
    recorded as ``unknown`` to keep label cardinality bounded.
    """

    def __init__(self, metrics: ServerMetrics) -> None:
        self._metrics = metrics

    async def __call__(
        self, ctx: ServerRequestContext[Any, Any], call_next: CallNext
    ) -> HandlerResult:
        if ctx.method != "tools/call":
            return await call_next(ctx)
        name = str((ctx.params or {}).get("name", ""))
        tool = name if name in TOOL_SCOPES else UNKNOWN_TOOL
        started = time.perf_counter()
        try:
            result = await call_next(ctx)
        except BaseException:
            self._metrics.record_tool_call(tool, "crash", time.perf_counter() - started)
            raise
        outcome = "error" if _is_error(result) else "success"
        self._metrics.record_tool_call(tool, outcome, time.perf_counter() - started)
        return result


def _is_error(result: HandlerResult) -> bool:
    if isinstance(result, CallToolResult):
        return bool(result.is_error)
    if isinstance(result, dict):
        return bool(result.get("isError") or result.get("is_error"))
    return False
