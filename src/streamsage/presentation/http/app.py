"""Streamable HTTP application: the MCP app behind bearer authentication."""

from mcp.server.mcpserver import MCPServer

from streamsage.infrastructure.config.settings import Settings
from streamsage.presentation.http.bearer_auth import (
    ASGIApp,
    BearerAuthMiddleware,
    StaticTokenAuthenticator,
    TokenRecord,
)

MCP_PATH = "/mcp"


def authenticator_from(settings: Settings) -> StaticTokenAuthenticator:
    return StaticTokenAuthenticator(
        [
            TokenRecord(name=t.name, sha256=t.sha256, scopes=frozenset(t.scopes))
            for t in settings.http_tokens
        ]
    )


def build_http_app(server: MCPServer, settings: Settings) -> ASGIApp:
    # `host` lets the SDK enable DNS-rebinding protection for loopback binds.
    mcp_app = server.streamable_http_app(
        streamable_http_path=MCP_PATH, host=settings.http_host
    )
    return BearerAuthMiddleware(mcp_app, authenticator_from(settings))
