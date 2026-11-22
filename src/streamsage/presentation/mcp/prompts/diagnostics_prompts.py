from typing import Annotated

from mcp.server.mcpserver import MCPServer
from pydantic import Field

_BASE_INSTRUCTIONS = """\
You are investigating an Apache Kafka cluster through the StreamSage MCP
server. StreamSage is read-only: it can observe the cluster but cannot change
anything in it.

Follow these steps:

1. Call `get_cluster_info` to see the cluster id, brokers and controller.
2. Call `list_topics` to see the available topics (pass
   `include_internal: true` only if internal topics are relevant).
3. For topics that look relevant, call `describe_topic` and inspect each
   partition's leader, replicas and in-sync replicas (ISR). Partitions
   without a leader or with fewer in-sync replicas than replicas deserve
   attention.
4. Call `list_consumer_groups`, then `analyze_consumer_health` for the
   groups that matter. Use `get_consumer_lag` for per-partition detail and
   `describe_consumer_group` to see members and assignments. Quote each
   finding's evidence and repeat its limitations.
   A lag of null means the offset is unknown, not zero. Lag is a single
   snapshot: high lag alone does not prove a consumer is stuck, since that
   requires seeing offsets fail to advance over time.
5. Report your findings in two clearly separated sections:
   - **Observed**: facts returned by the tools, quoting the relevant values.
   - **Hypotheses**: possible explanations, each marked as unverified.
6. Do not claim the cluster is fully healthy based on this data. It does
   not show throughput, disk usage, broker resource pressure or client
   errors. State which data you did not have.
"""


def build_investigation_prompt(focus_topic: str | None = None) -> str:
    if not focus_topic:
        return _BASE_INSTRUCTIONS
    return (
        _BASE_INSTRUCTIONS
        + f"\nPay particular attention to the topic `{focus_topic}`: describe it "
        "and explain what its partition metadata shows.\n"
    )


def register_diagnostics_prompts(server: MCPServer) -> None:
    @server.prompt(
        name="investigate_kafka_cluster",
        title="Investigate Kafka cluster",
        description=(
            "Guided, evidence-based investigation of the Kafka cluster's "
            "metadata using StreamSage's read-only tools."
        ),
    )
    def investigate_kafka_cluster(
        focus_topic: Annotated[
            str | None,
            Field(description="Optional topic to investigate in more depth."),
        ] = None,
    ) -> str:
        return build_investigation_prompt(focus_topic)
