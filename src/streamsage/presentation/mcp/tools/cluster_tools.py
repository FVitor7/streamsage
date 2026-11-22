from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from streamsage.application.use_cases.get_cluster_info import GetClusterInfoUseCase
from streamsage.presentation.mcp.errors import operation_boundary
from streamsage.presentation.mcp.schemas import ClusterInfoResponse
from streamsage.presentation.mcp.tools.annotations import READ_ONLY


def register_cluster_tools(
    server: MCPServer, get_cluster_info_use_case: GetClusterInfoUseCase
) -> None:
    @server.tool(
        name="get_cluster_info",
        title="Get Kafka cluster info",
        description=(
            "Return Kafka cluster metadata: cluster id, brokers (id, host, port) "
            "and the controller id when the cluster reports one. This is a single "
            "metadata snapshot; it does not prove the cluster is healthy."
        ),
        annotations=READ_ONLY,
    )
    def get_cluster_info(ctx: Context) -> ClusterInfoResponse:
        with operation_boundary("get_cluster_info", ToolError, ctx.request_id):
            cluster = get_cluster_info_use_case.execute()
        return ClusterInfoResponse.from_domain(cluster)
