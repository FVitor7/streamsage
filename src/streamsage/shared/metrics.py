"""Prometheus metrics about the StreamSage server itself.

Metrics live in a dedicated registry created by the composition root (no
process-global state), and every label has a bounded set of values: tool and
operation names come from code, never from client input.
"""

import time
from collections.abc import Iterator
from contextlib import contextmanager

from prometheus_client import CollectorRegistry, Counter, Histogram, Info

_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 90)


class ServerMetrics:
    def __init__(self, version: str, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.tool_calls = Counter(
            "streamsage_tool_calls",
            "MCP tool calls by tool and outcome (success, error, crash).",
            ["tool", "outcome"],
            registry=self.registry,
        )
        self.tool_duration = Histogram(
            "streamsage_tool_duration_seconds",
            "MCP tool call duration.",
            ["tool"],
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.kafka_requests = Counter(
            "streamsage_kafka_requests",
            "Kafka requests by operation and outcome (success, not_found, failure).",
            ["operation", "outcome"],
            registry=self.registry,
        )
        self.kafka_duration = Histogram(
            "streamsage_kafka_request_duration_seconds",
            "Kafka request duration.",
            ["operation"],
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.http_auth_failures = Counter(
            "streamsage_http_auth_failures",
            "Rejected HTTP requests by reason (missing_token, invalid_token).",
            ["reason"],
            registry=self.registry,
        )
        self.authorization_denials = Counter(
            "streamsage_authorization_denials",
            "MCP requests denied for a missing scope, by target.",
            ["target"],
            registry=self.registry,
        )
        Info(
            "streamsage_build", "StreamSage build information.", registry=self.registry
        ).info({"version": version})

    def record_tool_call(self, tool: str, outcome: str, seconds: float) -> None:
        self.tool_calls.labels(tool=tool, outcome=outcome).inc()
        self.tool_duration.labels(tool=tool).observe(seconds)

    @contextmanager
    def kafka_request(self, operation: str) -> Iterator["KafkaRequestTimer"]:
        """Time one Kafka request; the outcome defaults to failure on error."""
        timer = KafkaRequestTimer()
        started = time.perf_counter()
        try:
            yield timer
        except BaseException:
            if timer.outcome == "success":
                timer.outcome = "failure"
            raise
        finally:
            self.kafka_requests.labels(operation=operation, outcome=timer.outcome).inc()
            self.kafka_duration.labels(operation=operation).observe(
                time.perf_counter() - started
            )


class KafkaRequestTimer:
    """Lets the caller refine the outcome (for example ``not_found``)."""

    def __init__(self) -> None:
        self.outcome = "success"
