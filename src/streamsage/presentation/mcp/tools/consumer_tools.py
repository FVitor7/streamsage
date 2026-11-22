from typing import Annotated

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from streamsage.application.use_cases.analyze_consumer_health import (
    AnalyzeConsumerHealthUseCase,
)
from streamsage.application.use_cases.describe_consumer_group import (
    DescribeConsumerGroupUseCase,
)
from streamsage.application.use_cases.detect_stalled_partitions import (
    DetectStalledPartitionsUseCase,
)
from streamsage.application.use_cases.get_consumer_lag import GetConsumerLagUseCase
from streamsage.application.use_cases.list_consumer_groups import (
    ListConsumerGroupsUseCase,
)
from streamsage.domain.diagnostics.consumer_progress import (
    MAX_SAMPLE_INTERVAL_SECONDS,
    MIN_SAMPLE_INTERVAL_SECONDS,
)
from streamsage.domain.entities.consumer_group import ConsumerGroupState
from streamsage.domain.value_objects.group_id import MAX_GROUP_ID_LENGTH
from streamsage.presentation.mcp.errors import operation_boundary
from streamsage.presentation.mcp.schemas import (
    ConsumerGroupDetailResponse,
    ConsumerGroupListResponse,
    ConsumerHealthResponse,
    ConsumerLagResponse,
    ConsumerProgressResponse,
)
from streamsage.presentation.mcp.tools.annotations import READ_ONLY

DEFAULT_MAX_PARTITIONS = 100
DEFAULT_SAMPLE_INTERVAL_SECONDS = 10
MAX_PARTITIONS_LIMIT = 1000

GroupId = Annotated[
    str,
    Field(
        description="Exact consumer group id.",
        min_length=1,
        max_length=MAX_GROUP_ID_LENGTH,
    ),
]


def register_consumer_tools(
    server: MCPServer,
    list_groups_use_case: ListConsumerGroupsUseCase,
    describe_group_use_case: DescribeConsumerGroupUseCase,
    get_lag_use_case: GetConsumerLagUseCase,
    analyze_health_use_case: AnalyzeConsumerHealthUseCase,
    detect_stalled_use_case: DetectStalledPartitionsUseCase,
) -> None:
    @server.tool(
        name="list_consumer_groups",
        title="List consumer groups",
        description=(
            "List Kafka consumer groups sorted by id, with their state. "
            "Optionally filter by one or more states."
        ),
        annotations=READ_ONLY,
    )
    def list_consumer_groups(
        states: Annotated[
            list[ConsumerGroupState] | None,
            Field(description="Only return groups in these states."),
        ] = None,
        *,
        ctx: Context,
    ) -> ConsumerGroupListResponse:
        with operation_boundary("list_consumer_groups", ToolError, ctx.request_id):
            groups = list_groups_use_case.execute(states=states)
        return ConsumerGroupListResponse.from_domain(groups)

    @server.tool(
        name="describe_consumer_group",
        title="Describe a consumer group",
        description=(
            "Describe one consumer group: state, partition assignor, coordinator "
            "and each member with its assigned partitions."
        ),
        annotations=READ_ONLY,
    )
    def describe_consumer_group(
        group_id: GroupId, ctx: Context
    ) -> ConsumerGroupDetailResponse:
        with operation_boundary("describe_consumer_group", ToolError, ctx.request_id):
            group = describe_group_use_case.execute(group_id)
        return ConsumerGroupDetailResponse.from_domain(group)

    @server.tool(
        name="get_consumer_lag",
        title="Get consumer group lag",
        description=(
            "Per-partition lag of a consumer group (log end offset minus committed "
            "offset), largest first, with total and maximum lag. Partitions with "
            "no committed offset have unknown (null) lag, never 0. Lag is a single "
            "snapshot: high lag alone does not prove a consumer is stuck."
        ),
        annotations=READ_ONLY,
    )
    def get_consumer_lag(
        group_id: GroupId,
        max_partitions: Annotated[
            int,
            Field(
                description="Maximum number of partitions to list.",
                ge=1,
                le=MAX_PARTITIONS_LIMIT,
            ),
        ] = DEFAULT_MAX_PARTITIONS,
        *,
        ctx: Context,
    ) -> ConsumerLagResponse:
        with operation_boundary("get_consumer_lag", ToolError, ctx.request_id):
            lag = get_lag_use_case.execute(group_id)
        return ConsumerLagResponse.from_domain(lag, max_partitions=max_partitions)

    @server.tool(
        name="analyze_consumer_health",
        title="Analyze consumer group health",
        description=(
            "Classify a consumer group's health (healthy, warning, critical or "
            "unknown) from its state, members and current lag, using the "
            "server's configured lag thresholds. Returns each finding with the "
            "evidence behind it and the limitations of the assessment. It uses "
            "a single snapshot, so it never claims a consumer is stuck."
        ),
        annotations=READ_ONLY,
    )
    def analyze_consumer_health(
        group_id: GroupId, ctx: Context
    ) -> ConsumerHealthResponse:
        with operation_boundary("analyze_consumer_health", ToolError, ctx.request_id):
            report = analyze_health_use_case.execute(group_id)
        return ConsumerHealthResponse.from_domain(report)

    @server.tool(
        name="detect_stalled_partitions",
        title="Detect stalled partitions",
        description=(
            "Detect partitions whose committed offsets are not advancing. Samples "
            "the group's offsets twice, interval_seconds apart, within this call, "
            "and classifies each partition as advancing, caught_up, not_advancing "
            "(pending messages but no commits), moved_backward or unknown. The "
            "call takes at least interval_seconds. A consumer that commits less "
            "often than the interval can look stalled."
        ),
        annotations=READ_ONLY,
    )
    def detect_stalled_partitions(
        group_id: GroupId,
        interval_seconds: Annotated[
            int,
            Field(
                description="Seconds between the two samples.",
                ge=MIN_SAMPLE_INTERVAL_SECONDS,
                le=MAX_SAMPLE_INTERVAL_SECONDS,
            ),
        ] = DEFAULT_SAMPLE_INTERVAL_SECONDS,
        max_partitions: Annotated[
            int,
            Field(
                description="Maximum number of partitions to list.",
                ge=1,
                le=MAX_PARTITIONS_LIMIT,
            ),
        ] = DEFAULT_MAX_PARTITIONS,
        *,
        ctx: Context,
    ) -> ConsumerProgressResponse:
        with operation_boundary("detect_stalled_partitions", ToolError, ctx.request_id):
            report = detect_stalled_use_case.execute(group_id, interval_seconds)
        return ConsumerProgressResponse.from_domain(
            report, max_partitions=max_partitions
        )
