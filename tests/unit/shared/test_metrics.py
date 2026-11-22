from types import SimpleNamespace
from typing import Any

import pytest
from mcp.types import CallToolResult, TextContent

from streamsage.presentation.mcp.metrics_middleware import (
    UNKNOWN_TOOL,
    MetricsMiddleware,
)
from streamsage.shared.metrics import ServerMetrics


def _value(metrics: ServerMetrics, name: str, **labels: str) -> float:
    return metrics.registry.get_sample_value(name, labels) or 0.0


def test_registries_are_isolated() -> None:
    first, second = ServerMetrics("1.0"), ServerMetrics("1.0")
    first.record_tool_call("list_topics", "success", 0.1)
    assert (
        _value(
            first, "streamsage_tool_calls_total", tool="list_topics", outcome="success"
        )
        == 1
    )
    assert (
        _value(
            second, "streamsage_tool_calls_total", tool="list_topics", outcome="success"
        )
        == 0
    )


def test_build_info() -> None:
    metrics = ServerMetrics("9.9.9")
    assert _value(metrics, "streamsage_build_info", version="9.9.9") == 1


def test_kafka_request_outcomes() -> None:
    metrics = ServerMetrics("1.0")
    with metrics.kafka_request("topic listing"):
        pass
    with metrics.kafka_request("topic description") as timer:
        timer.outcome = "not_found"
    with pytest.raises(RuntimeError), metrics.kafka_request("topic listing"):
        raise RuntimeError("down")

    def count(operation: str, outcome: str) -> float:
        return _value(
            metrics,
            "streamsage_kafka_requests_total",
            operation=operation,
            outcome=outcome,
        )

    assert count("topic listing", "success") == 1
    assert count("topic listing", "failure") == 1
    assert count("topic description", "not_found") == 1
    assert (
        _value(
            metrics,
            "streamsage_kafka_request_duration_seconds_count",
            operation="topic listing",
        )
        == 2
    )


async def _middleware(
    metrics: ServerMetrics, ctx: Any, result: Any = None, error: Exception | None = None
) -> Any:
    async def call_next(_ctx: Any) -> Any:
        if error is not None:
            raise error
        return result

    return await MetricsMiddleware(metrics)(ctx, call_next)


def _call(name: str) -> Any:
    return SimpleNamespace(method="tools/call", params={"name": name}, request=None)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("result", "outcome"),
    [
        (CallToolResult(content=[TextContent(type="text", text="ok")]), "success"),
        (
            CallToolResult(content=[TextContent(type="text", text="x")], is_error=True),
            "error",
        ),
        ({"content": [], "isError": True}, "error"),
        ({"content": []}, "success"),
    ],
)
async def test_tool_call_outcomes(result: Any, outcome: str) -> None:
    metrics = ServerMetrics("1.0")
    assert await _middleware(metrics, _call("list_topics"), result) is result
    assert (
        _value(
            metrics, "streamsage_tool_calls_total", tool="list_topics", outcome=outcome
        )
        == 1
    )
    assert (
        _value(metrics, "streamsage_tool_duration_seconds_count", tool="list_topics")
        == 1
    )


@pytest.mark.anyio
async def test_crashes_are_counted_and_reraised() -> None:
    metrics = ServerMetrics("1.0")
    with pytest.raises(RuntimeError):
        await _middleware(metrics, _call("list_topics"), error=RuntimeError("bug"))
    assert (
        _value(
            metrics, "streamsage_tool_calls_total", tool="list_topics", outcome="crash"
        )
        == 1
    )


@pytest.mark.anyio
async def test_unknown_tool_names_do_not_create_new_label_values() -> None:
    metrics = ServerMetrics("1.0")
    for name in ("x1", "x2", "../../etc"):
        await _middleware(metrics, _call(name), {"content": []})
    assert (
        _value(
            metrics, "streamsage_tool_calls_total", tool=UNKNOWN_TOOL, outcome="success"
        )
        == 3
    )
    assert (
        _value(metrics, "streamsage_tool_calls_total", tool="x1", outcome="success")
        == 0
    )


@pytest.mark.anyio
async def test_other_methods_are_not_recorded() -> None:
    metrics = ServerMetrics("1.0")
    ctx = SimpleNamespace(method="tools/list", params=None, request=None)
    await _middleware(metrics, ctx, {"tools": []})
    samples = [
        s
        for m in metrics.registry.collect()
        if m.name == "streamsage_tool_calls"
        for s in m.samples
    ]
    assert samples == []
