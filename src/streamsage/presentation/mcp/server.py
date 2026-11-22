"""Construction of the StreamSage MCP server.

The server is a thin adapter: it validates arguments, calls use cases and
serializes results. It holds no Kafka client and no business rules.
"""

from mcp.server.mcpserver import MCPServer

from streamsage import __version__
from streamsage.application.use_cases.analyze_consumer_health import (
    AnalyzeConsumerHealthUseCase,
)
from streamsage.application.use_cases.describe_consumer_group import (
    DescribeConsumerGroupUseCase,
)
from streamsage.application.use_cases.describe_topic import DescribeTopicUseCase
from streamsage.application.use_cases.detect_stalled_partitions import (
    DetectStalledPartitionsUseCase,
)
from streamsage.application.use_cases.get_cluster_info import GetClusterInfoUseCase
from streamsage.application.use_cases.get_consumer_lag import GetConsumerLagUseCase
from streamsage.application.use_cases.inspect_messages import InspectMessagesUseCase
from streamsage.application.use_cases.list_consumer_groups import (
    ListConsumerGroupsUseCase,
)
from streamsage.application.use_cases.list_topics import ListTopicsUseCase
from streamsage.presentation.mcp.prompts.diagnostics_prompts import (
    register_diagnostics_prompts,
)
from streamsage.presentation.mcp.resources.cluster_resources import (
    register_cluster_resources,
)
from streamsage.presentation.mcp.tools.cluster_tools import register_cluster_tools
from streamsage.presentation.mcp.tools.consumer_tools import register_consumer_tools
from streamsage.presentation.mcp.tools.message_tools import register_message_tools
from streamsage.presentation.mcp.tools.topic_tools import register_topic_tools

SERVER_NAME = "streamsage"

INSTRUCTIONS = """\
StreamSage exposes read-only Kafka cluster data. Use get_cluster_info,
list_topics and describe_topic to explore the cluster, and
list_consumer_groups, describe_consumer_group, get_consumer_lag and
analyze_consumer_health to inspect consumers. Lag is a single snapshot: high
lag alone does not prove a consumer is stuck; detect_stalled_partitions
compares offsets over time. inspect_messages reads a bounded, sanitized
sample of messages. No tool can modify the cluster. Distinguish
observed data from hypotheses, and never state a root cause the evidence
does not establish.
"""


def create_server(
    *,
    get_cluster_info_use_case: GetClusterInfoUseCase,
    list_topics_use_case: ListTopicsUseCase,
    describe_topic_use_case: DescribeTopicUseCase,
    list_consumer_groups_use_case: ListConsumerGroupsUseCase,
    describe_consumer_group_use_case: DescribeConsumerGroupUseCase,
    get_consumer_lag_use_case: GetConsumerLagUseCase,
    analyze_consumer_health_use_case: AnalyzeConsumerHealthUseCase,
    detect_stalled_partitions_use_case: DetectStalledPartitionsUseCase,
    inspect_messages_use_case: InspectMessagesUseCase,
    message_default_limit: int = 20,
    message_max_limit: int = 100,
    message_payloads_enabled: bool = False,
    include_internal_topics: bool = False,
) -> MCPServer:
    server: MCPServer = MCPServer(
        name=SERVER_NAME,
        title="StreamSage",
        description="AI-powered Kafka observability and troubleshooting.",
        instructions=INSTRUCTIONS,
        version=__version__,
    )
    register_cluster_tools(server, get_cluster_info_use_case)
    register_topic_tools(
        server,
        list_topics_use_case,
        describe_topic_use_case,
        include_internal_by_default=include_internal_topics,
    )
    register_consumer_tools(
        server,
        list_consumer_groups_use_case,
        describe_consumer_group_use_case,
        get_consumer_lag_use_case,
        analyze_consumer_health_use_case,
        detect_stalled_partitions_use_case,
    )
    register_message_tools(
        server,
        inspect_messages_use_case,
        default_limit=message_default_limit,
        max_limit=message_max_limit,
        payloads_enabled=message_payloads_enabled,
    )
    register_cluster_resources(
        server,
        get_cluster_info_use_case,
        list_topics_use_case,
        include_internal_topics=include_internal_topics,
    )
    register_diagnostics_prompts(server)
    return server
