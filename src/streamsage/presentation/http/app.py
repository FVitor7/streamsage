"""HTTP application: MCP, /metrics and /healthz behind bearer authentication."""

from mcp.server.mcpserver import MCPServer
from prometheus_client import make_asgi_app

from streamsage.infrastructure.config.settings import Settings
from streamsage.presentation.http.bearer_auth import (
    ASGIApp,
    BearerAuthMiddleware,
    Receive,
    Scope,
    Send,
    StaticTokenAuthenticator,
    TokenRecord,
)
from streamsage.shared.metrics import ServerMetrics

MCP_PATH = "/mcp"
METRICS_PATH = "/metrics"
HEALTH_PATH = "/healthz"


def authenticator_from(settings: Settings) -> StaticTokenAuthenticator:
    return StaticTokenAuthenticator(
        [
            TokenRecord(name=t.name, sha256=t.sha256, scopes=frozenset(t.scopes))
            for t in settings.http_tokens
        ]
    )


def build_http_app(
    server: MCPServer, settings: Settings, metrics: ServerMetrics | None = None
) -> ASGIApp:
    # `host` lets the SDK enable DNS-rebinding protection for loopback binds.
    mcp_app = server.streamable_http_app(
        streamable_http_path=MCP_PATH, host=settings.http_host
    )
    metrics_app = make_asgi_app(metrics.registry) if metrics is not None else None
    router = _Router(mcp_app, metrics_app)
    public = {HEALTH_PATH}
    if settings.http_metrics_public:
        public.add(METRICS_PATH)
    return BearerAuthMiddleware(
        router, authenticator_from(settings), frozenset(public), metrics
    )


class _Router:
    """Serves /healthz and /metrics; everything else goes to the MCP app."""

    def __init__(self, mcp_app: ASGIApp, metrics_app: ASGIApp | None) -> None:
        self._mcp_app = mcp_app
        self._metrics_app = metrics_app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path") if scope["type"] == "http" else None
        if path == HEALTH_PATH:
            await _plain(send, 200, b"ok")
        elif path == METRICS_PATH and self._metrics_app is not None:
            await self._metrics_app(scope, receive, send)
        else:
            await self._mcp_app(scope, receive, send)


async def _plain(send: Send, status: int, body: bytes) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
