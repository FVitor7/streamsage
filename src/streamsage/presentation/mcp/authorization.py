"""Scope-based authorization of MCP requests received over HTTP.

Every tool requires one scope. Reading message contents is more sensitive
than reading metadata, so it has its own scope. Unmapped tools are denied
(fail closed), and tools/list only shows what the caller may call.

Requests that did not arrive over HTTP (stdio) carry no HTTP request: the
client is a local process started by the user, so they are not restricted.
"""

import logging
from dataclasses import dataclass
from typing import Any

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.types import CallToolResult, ListToolsResult, TextContent

from streamsage.shared.metrics import ServerMetrics

logger = logging.getLogger("streamsage.auth")

METADATA_SCOPE = "kafka:metadata"
MESSAGES_SCOPE = "kafka:messages"
ALL_SCOPES = frozenset({METADATA_SCOPE, MESSAGES_SCOPE})

TOOL_SCOPES: dict[str, str] = {
    "get_cluster_info": METADATA_SCOPE,
    "list_topics": METADATA_SCOPE,
    "describe_topic": METADATA_SCOPE,
    "list_consumer_groups": METADATA_SCOPE,
    "describe_consumer_group": METADATA_SCOPE,
    "get_consumer_lag": METADATA_SCOPE,
    "analyze_consumer_health": METADATA_SCOPE,
    "detect_stalled_partitions": METADATA_SCOPE,
    "list_dlq_topics": METADATA_SCOPE,
    "inspect_messages": MESSAGES_SCOPE,
    "find_messages_by_key": MESSAGES_SCOPE,
}
# Resources expose cluster and topic metadata.
RESOURCE_SCOPE = METADATA_SCOPE

# Key under which the HTTP authentication middleware stores the principal in
# the ASGI scope of each request.
PRINCIPAL_SCOPE_KEY = "streamsage.principal"


@dataclass(frozen=True, slots=True)
class Principal:
    """An authenticated HTTP caller. ``name`` identifies the token, never its value."""

    name: str
    scopes: frozenset[str]

    def allows(self, scope: str | None) -> bool:
        return scope is not None and scope in self.scopes


class ScopeAuthorizationMiddleware:
    """MCP server middleware enforcing ``TOOL_SCOPES`` on HTTP requests."""

    def __init__(self, metrics: ServerMetrics | None = None) -> None:
        self._metrics = metrics

    async def __call__(
        self, ctx: ServerRequestContext[Any, Any], call_next: CallNext
    ) -> HandlerResult:
        request = ctx.request
        if request is None:
            return await call_next(ctx)  # stdio: local, trusted client
        principal = request.scope.get(PRINCIPAL_SCOPE_KEY)
        if not isinstance(principal, Principal):
            # The HTTP request bypassed authentication: allow nothing.
            principal = Principal(name="<unauthenticated>", scopes=frozenset())

        if ctx.method == "tools/call":
            tool = str((ctx.params or {}).get("name", ""))
            required = TOOL_SCOPES.get(tool)
            if not principal.allows(required):
                self._count_denial(tool if tool in TOOL_SCOPES else "unknown")
                return _deny(principal, tool, required)
        if ctx.method in {"resources/read", "resources/list"} and not principal.allows(
            RESOURCE_SCOPE
        ):
            self._count_denial(ctx.method)
            return _deny(principal, ctx.method, RESOURCE_SCOPE)

        result = await call_next(ctx)
        if ctx.method == "tools/list":
            return _visible_tools(result, principal)
        return result

    def _count_denial(self, target: str) -> None:
        if self._metrics is not None:
            self._metrics.authorization_denials.labels(target=target).inc()


def _visible_tools(result: HandlerResult, principal: Principal) -> HandlerResult:
    """Keep only the tools ``principal`` may call.

    Depending on the protocol version, the SDK hands middleware either a
    ``ListToolsResult`` or its already-serialized dict form; both are
    filtered. Anything else passes through unchanged.
    """
    if isinstance(result, ListToolsResult):
        result.tools = [
            tool
            for tool in result.tools
            if principal.allows(TOOL_SCOPES.get(tool.name))
        ]
        return result
    if isinstance(result, dict) and isinstance(result.get("tools"), list):
        return {
            **result,
            "tools": [
                tool
                for tool in result["tools"]
                if isinstance(tool, dict)
                and principal.allows(TOOL_SCOPES.get(str(tool.get("name"))))
            ],
        }
    return result


def _deny(principal: Principal, target: str, required: str | None) -> CallToolResult:
    logger.warning(
        "request denied: missing scope",
        extra={
            "event": {
                "principal": principal.name,
                "target": target,
                "required_scope": required,
            }
        },
    )
    reason = (
        f"This token is not allowed to use '{target}' (requires scope '{required}')."
        if required
        else f"'{target}' is not available over HTTP."
    )
    return CallToolResult(
        content=[TextContent(type="text", text=reason)], is_error=True
    )
