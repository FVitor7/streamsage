from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ResourceError

from streamsage.application.use_cases.get_cluster_info import GetClusterInfoUseCase
from streamsage.application.use_cases.list_topics import ListTopicsUseCase
from streamsage.presentation.mcp.errors import operation_boundary
from streamsage.presentation.mcp.schemas import (
    ClusterInfoResponse,
    TopicListResponse,
)

JSON = "application/json"


def register_cluster_resources(
    server: MCPServer,
    get_cluster_info_use_case: GetClusterInfoUseCase,
    list_topics_use_case: ListTopicsUseCase,
    *,
    include_internal_topics: bool,
) -> None:
    @server.resource(
        "kafka://cluster/info",
        name="cluster_info",
        title="Kafka cluster info",
        description="Cluster id, brokers and controller id (JSON).",
        mime_type=JSON,
    )
    def cluster_info() -> str:
        with operation_boundary("resource:kafka://cluster/info", ResourceError):
            cluster = get_cluster_info_use_case.execute()
        return ClusterInfoResponse.from_domain(cluster).model_dump_json()

    @server.resource(
        "kafka://topics",
        name="topics",
        title="Kafka topics",
        description=(
            "Topics with partition count and replication factor (JSON). "
            "Internal topics follow the server's include_internal_topics setting."
        ),
        mime_type=JSON,
    )
    def topics() -> str:
        with operation_boundary("resource:kafka://topics", ResourceError):
            topic_list = list_topics_use_case.execute(
                include_internal=include_internal_topics
            )
        return TopicListResponse.from_domain(
            topic_list, include_internal=include_internal_topics
        ).model_dump_json()
