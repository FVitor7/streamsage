"""Static bearer-token authentication for the HTTP transport.

The operator configures tokens by their SHA-256 hash (never the token
itself) and the scopes each one grants. Tokens are long random strings
created with ``streamsage generate-token``, so a fast hash is sufficient.
Comparison is constant-time, and token values are never logged.
"""

import hashlib
import hmac
import json
import logging
import secrets
from collections.abc import Awaitable, Callable, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Any

from streamsage.presentation.mcp.authorization import PRINCIPAL_SCOPE_KEY, Principal
from streamsage.shared.metrics import ServerMetrics

logger = logging.getLogger("streamsage.auth")

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

TOKEN_PREFIX = "ssk_"


@dataclass(frozen=True, slots=True)
class TokenRecord:
    name: str
    sha256: str
    scopes: frozenset[str]


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def generate_token() -> str:
    return TOKEN_PREFIX + secrets.token_urlsafe(32)


class StaticTokenAuthenticator:
    def __init__(self, records: Sequence[TokenRecord]) -> None:
        self._records = tuple(records)

    def authenticate(self, token: str) -> Principal | None:
        digest = hash_token(token)
        match: TokenRecord | None = None
        # Compare against every record so timing does not reveal which (or
        # how many) records exist.
        for record in self._records:
            if hmac.compare_digest(digest, record.sha256):
                match = record
        if match is None:
            return None
        return Principal(name=match.name, scopes=match.scopes)


class BearerAuthMiddleware:
    """ASGI middleware requiring ``Authorization: Bearer <token>``."""

    def __init__(
        self,
        app: ASGIApp,
        authenticator: StaticTokenAuthenticator,
        public_paths: frozenset[str] = frozenset(),
        metrics: ServerMetrics | None = None,
    ) -> None:
        self._app = app
        self._authenticator = authenticator
        self._public_paths = public_paths
        self._metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self._app(scope, receive, send)
            return
        if scope["type"] != "http":
            await _reject(send, 404, "not_found")
            return
        if scope.get("path") in self._public_paths:
            await self._app(scope, receive, send)
            return

        token = _bearer_token(scope)
        principal = self._authenticator.authenticate(token) if token else None
        if principal is None:
            reason = "invalid_token" if token else "missing_token"
            if self._metrics is not None:
                self._metrics.http_auth_failures.labels(reason=reason).inc()
            client = scope.get("client") or ("?", 0)
            logger.warning(
                "authentication failed",
                extra={
                    "event": {
                        "reason": "invalid_token" if token else "missing_token",
                        "client": client[0],
                        "path": scope.get("path"),
                    }
                },
            )
            await _reject(send, 401, "invalid_token" if token else "missing_token")
            return
        scope[PRINCIPAL_SCOPE_KEY] = principal
        await self._app(scope, receive, send)


def _bearer_token(scope: Scope) -> str | None:
    headers: list[tuple[bytes, bytes]] = scope.get("headers", [])
    for name, value in headers:
        if name.lower() == b"authorization":
            scheme, _, credentials = value.decode("latin-1").partition(" ")
            token = credentials.strip()
            if scheme.lower() == "bearer" and token:
                return token
            return None
    return None


async def _reject(send: Send, status: int, error: str) -> None:
    body = json.dumps({"error": error}).encode()
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode()),
    ]
    if status == 401:
        headers.append(
            (
                b"www-authenticate",
                f'Bearer realm="streamsage", error="{error}"'.encode(),
            )
        )
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})
