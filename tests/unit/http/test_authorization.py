from types import SimpleNamespace
from typing import Any

import pytest
from mcp.types import CallToolResult, ListToolsResult, Tool

from streamsage.infrastructure.config.settings import HttpScope
from streamsage.presentation.mcp.authorization import (
    ALL_SCOPES,
    MESSAGES_SCOPE,
    METADATA_SCOPE,
    PRINCIPAL_SCOPE_KEY,
    TOOL_SCOPES,
    Principal,
    ScopeAuthorizationMiddleware,
)

METADATA_ONLY = Principal("reader", frozenset({METADATA_SCOPE}))


def _ctx(method: str, params: dict[str, Any] | None, principal: object) -> Any:
    scope = {} if principal is None else {PRINCIPAL_SCOPE_KEY: principal}
    return SimpleNamespace(
        method=method, params=params, request=SimpleNamespace(scope=scope)
    )


def _tools(*names: str) -> ListToolsResult:
    return ListToolsResult(
        tools=[Tool(name=n, input_schema={"type": "object"}) for n in names]
    )


async def _run(ctx: Any, result: Any = "handled") -> Any:
    calls = []

    async def call_next(inner: Any) -> Any:
        calls.append(inner)
        return result

    outcome = await ScopeAuthorizationMiddleware()(ctx, call_next)
    return outcome, calls


@pytest.mark.anyio
async def test_stdio_requests_are_not_restricted() -> None:
    ctx = SimpleNamespace(
        method="tools/call", params={"name": "inspect_messages"}, request=None
    )
    outcome, calls = await _run(ctx)
    assert outcome == "handled"
    assert calls == [ctx]


@pytest.mark.anyio
async def test_tool_call_with_required_scope_is_allowed() -> None:
    outcome, calls = await _run(
        _ctx("tools/call", {"name": "get_consumer_lag"}, METADATA_ONLY)
    )
    assert outcome == "handled"


@pytest.mark.anyio
@pytest.mark.parametrize("tool", ["inspect_messages", "find_messages_by_key"])
async def test_message_tools_require_the_messages_scope(tool: str) -> None:
    outcome, calls = await _run(_ctx("tools/call", {"name": tool}, METADATA_ONLY))
    assert calls == []
    assert isinstance(outcome, CallToolResult)
    assert outcome.is_error is True
    assert "kafka:messages" in outcome.content[0].text  # type: ignore[union-attr]


@pytest.mark.anyio
async def test_unknown_tools_are_denied() -> None:
    full = Principal("admin", ALL_SCOPES)
    outcome, calls = await _run(_ctx("tools/call", {"name": "drop_topic"}, full))
    assert calls == []
    assert "not available over HTTP" in outcome.content[0].text


@pytest.mark.anyio
async def test_requests_without_principal_are_denied() -> None:
    outcome, calls = await _run(_ctx("tools/call", {"name": "list_topics"}, None))
    assert calls == []
    outcome, _ = await _run(_ctx("tools/list", None, None), _tools("list_topics"))
    assert outcome.tools == []


@pytest.mark.anyio
async def test_tool_list_is_filtered_by_scope() -> None:
    listing = _tools("list_topics", "inspect_messages", "unmapped")
    outcome, _ = await _run(_ctx("tools/list", None, METADATA_ONLY), listing)
    assert [t.name for t in outcome.tools] == ["list_topics"]


@pytest.mark.anyio
async def test_resources_require_the_metadata_scope() -> None:
    messages_only = Principal("m", frozenset({MESSAGES_SCOPE}))
    outcome, calls = await _run(_ctx("resources/read", {}, messages_only))
    assert calls == []
    outcome, calls = await _run(_ctx("resources/list", {}, METADATA_ONLY))
    assert outcome == "handled"


@pytest.mark.anyio
async def test_prompts_and_other_methods_pass_for_any_principal() -> None:
    nobody = Principal("n", frozenset())
    outcome, _ = await _run(_ctx("prompts/list", None, nobody))
    assert outcome == "handled"


def test_settings_scopes_match_authorization_scopes() -> None:
    assert set(HttpScope.__args__) == set(ALL_SCOPES)  # type: ignore[attr-defined]
    assert set(TOOL_SCOPES.values()) <= ALL_SCOPES


@pytest.mark.anyio
async def test_serialized_tool_list_is_filtered_too() -> None:
    # Protocol 2026-07-28: middleware receives the already-serialized result.
    serialized = {
        "resultType": "complete",
        "tools": [{"name": "list_topics"}, {"name": "inspect_messages"}, "junk"],
        "ttlMs": 0,
    }
    outcome, _ = await _run(_ctx("tools/list", None, METADATA_ONLY), serialized)
    assert outcome == {
        "resultType": "complete",
        "tools": [{"name": "list_topics"}],
        "ttlMs": 0,
    }


@pytest.mark.anyio
async def test_unexpected_list_result_shapes_pass_through() -> None:
    outcome, _ = await _run(_ctx("tools/list", None, METADATA_ONLY), {"other": 1})
    assert outcome == {"other": 1}


@pytest.mark.anyio
async def test_denials_are_counted_with_bounded_labels() -> None:
    from streamsage.shared.metrics import ServerMetrics

    metrics = ServerMetrics("1.0")
    middleware = ScopeAuthorizationMiddleware(metrics)

    async def call_next(_ctx: Any) -> Any:
        return "handled"

    for name in ("inspect_messages", "made-up-tool"):
        await middleware(_ctx("tools/call", {"name": name}, METADATA_ONLY), call_next)
    reg = metrics.registry
    assert (
        reg.get_sample_value(
            "streamsage_authorization_denials_total", {"target": "inspect_messages"}
        )
        == 1
    )
    assert (
        reg.get_sample_value(
            "streamsage_authorization_denials_total", {"target": "unknown"}
        )
        == 1
    )
