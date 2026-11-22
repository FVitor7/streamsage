from typing import Any

import pytest

from streamsage.presentation.http.bearer_auth import (
    TOKEN_PREFIX,
    BearerAuthMiddleware,
    StaticTokenAuthenticator,
    TokenRecord,
    generate_token,
    hash_token,
)
from streamsage.presentation.mcp.authorization import PRINCIPAL_SCOPE_KEY, Principal

TOKEN = generate_token()
AUTH = StaticTokenAuthenticator(
    [
        TokenRecord("ci", hash_token(TOKEN), frozenset({"kafka:metadata"})),
        TokenRecord(
            "other", hash_token(generate_token()), frozenset({"kafka:messages"})
        ),
    ]
)


def test_generated_tokens_are_random_and_prefixed() -> None:
    first, second = generate_token(), generate_token()
    assert first != second
    assert first.startswith(TOKEN_PREFIX)
    assert len(first) > 40


def test_authenticator_accepts_only_configured_tokens() -> None:
    assert AUTH.authenticate(TOKEN) == Principal("ci", frozenset({"kafka:metadata"}))
    assert AUTH.authenticate(TOKEN + "x") is None
    assert AUTH.authenticate("") is None
    assert hash_token(TOKEN) != TOKEN


class Recorder:
    def __init__(self) -> None:
        self.scopes: list[dict[str, Any]] = []

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        self.scopes.append(scope)


async def _call(
    middleware: BearerAuthMiddleware, scope: dict[str, Any]
) -> list[dict[str, Any]]:
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await middleware(scope, receive, send)
    return sent


def _http(headers: list[tuple[bytes, bytes]], path: str = "/mcp") -> dict[str, Any]:
    return {"type": "http", "path": path, "headers": headers, "client": ("1.2.3.4", 5)}


@pytest.mark.anyio
async def test_valid_token_reaches_the_app_with_a_principal() -> None:
    app = Recorder()
    sent = await _call(
        BearerAuthMiddleware(app, AUTH),
        _http([(b"authorization", f"Bearer {TOKEN}".encode())]),
    )
    assert sent == []
    assert app.scopes[0][PRINCIPAL_SCOPE_KEY].name == "ci"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("headers", "error"),
    [
        ([], "missing_token"),
        ([(b"authorization", b"Basic abc")], "missing_token"),
        ([(b"authorization", b"Bearer ")], "missing_token"),
        ([(b"authorization", b"Bearer wrong")], "invalid_token"),
    ],
)
async def test_rejected_requests_get_401_and_never_reach_the_app(
    headers: list[tuple[bytes, bytes]], error: str, caplog: pytest.LogCaptureFixture
) -> None:
    app = Recorder()
    sent = await _call(BearerAuthMiddleware(app, AUTH), _http(headers))
    assert app.scopes == []
    start = sent[0]
    assert start["status"] == 401
    challenge = dict(start["headers"])[b"www-authenticate"].decode()
    assert challenge == f'Bearer realm="streamsage", error="{error}"'
    assert error.encode() in sent[1]["body"]
    assert "wrong" not in caplog.text  # token values are never logged


@pytest.mark.anyio
async def test_scheme_is_case_insensitive() -> None:
    app = Recorder()
    await _call(
        BearerAuthMiddleware(app, AUTH),
        _http([(b"Authorization", f"bearer {TOKEN}".encode())]),
    )
    assert len(app.scopes) == 1


@pytest.mark.anyio
async def test_public_paths_and_lifespan_pass_through() -> None:
    app = Recorder()
    middleware = BearerAuthMiddleware(app, AUTH, public_paths=frozenset({"/healthz"}))
    await _call(middleware, _http([], path="/healthz"))
    await _call(middleware, {"type": "lifespan"})
    assert len(app.scopes) == 2


@pytest.mark.anyio
async def test_other_protocols_are_refused() -> None:
    app = Recorder()
    sent = await _call(BearerAuthMiddleware(app, AUTH), {"type": "websocket"})
    assert app.scopes == []
    assert sent[0]["status"] == 404


@pytest.mark.anyio
async def test_auth_failures_are_counted() -> None:
    from streamsage.shared.metrics import ServerMetrics

    metrics = ServerMetrics("1.0")
    middleware = BearerAuthMiddleware(Recorder(), AUTH, metrics=metrics)
    await _call(middleware, _http([]))
    await _call(middleware, _http([(b"authorization", b"Bearer wrong")]))
    await _call(middleware, _http([(b"authorization", f"Bearer {TOKEN}".encode())]))
    reg = metrics.registry
    assert (
        reg.get_sample_value(
            "streamsage_http_auth_failures_total", {"reason": "missing_token"}
        )
        == 1
    )
    assert (
        reg.get_sample_value(
            "streamsage_http_auth_failures_total", {"reason": "invalid_token"}
        )
        == 1
    )
