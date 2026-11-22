from typing import Annotated

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from streamsage.application.use_cases.find_messages_by_key import (
    MAX_SEARCH_KEY_BYTES,
    FindMessagesByKeyUseCase,
)
from streamsage.application.use_cases.inspect_messages import InspectMessagesUseCase
from streamsage.application.use_cases.list_dlq_topics import ListDlqTopicsUseCase
from streamsage.domain.value_objects.topic_name import (
    MAX_TOPIC_NAME_LENGTH,
    TOPIC_NAME_PATTERN,
)
from streamsage.presentation.mcp.errors import operation_boundary
from streamsage.presentation.mcp.schemas import (
    DlqOverviewResponse,
    DlqTopicResponse,
    KeySearchResponse,
    MessageInspectionResponse,
    MessageResponse,
    ScanCoverageResponse,
)
from streamsage.presentation.mcp.tools.annotations import READ_ONLY

TopicName = Annotated[
    str,
    Field(
        description="Exact name of an existing Kafka topic.",
        min_length=1,
        max_length=MAX_TOPIC_NAME_LENGTH,
        pattern=TOPIC_NAME_PATTERN,
    ),
]


def register_message_tools(
    server: MCPServer,
    inspect_use_case: InspectMessagesUseCase,
    find_by_key_use_case: FindMessagesByKeyUseCase,
    list_dlq_use_case: ListDlqTopicsUseCase,
    *,
    default_limit: int,
    max_limit: int,
    payloads_enabled: bool,
) -> None:
    payload_note = (
        "Payload previews are included, truncated and with sensitive JSON fields "
        "redacted."
        if payloads_enabled
        else "Payloads are omitted on this server (only their size is shown)."
    )

    @server.tool(
        name="inspect_messages",
        title="Inspect messages",
        description=(
            "Read a bounded number of messages from a topic: key, headers, "
            "timestamp, offset and payload size. By default returns the newest "
            "messages (newest first); with partition and start_offset, reads "
            "forward from that offset. Header values with sensitive names are "
            f"redacted. {payload_note} Never consumes as a group or commits "
            "offsets."
        ),
        annotations=READ_ONLY,
    )
    def inspect_messages(
        topic_name: TopicName,
        limit: Annotated[
            int, Field(description="Maximum messages to return.", ge=1, le=max_limit)
        ] = default_limit,
        partition: Annotated[
            int | None, Field(description="Only read this partition.", ge=0)
        ] = None,
        start_offset: Annotated[
            int | None,
            Field(description="Read forward from this offset (needs partition).", ge=0),
        ] = None,
        *,
        ctx: Context,
    ) -> MessageInspectionResponse:
        with operation_boundary("inspect_messages", ToolError, ctx.request_id):
            result = inspect_use_case.execute(
                topic_name, limit, partition=partition, start_offset=start_offset
            )
        return MessageInspectionResponse(
            topic=result.topic,
            payloads_included=result.payloads_included,
            max_payload_bytes=result.max_payload_bytes,
            message_count=len(result.messages),
            complete=result.complete,
            notes=list(result.notes),
            messages=[MessageResponse.from_domain(m) for m in result.messages],
        )

    @server.tool(
        name="find_messages_by_key",
        title="Find messages by key",
        description=(
            "Find the newest messages whose key exactly equals `key` (UTF-8). "
            "Kafka has no index by key, so only the newest messages of each "
            "partition are scanned, within server limits; the response reports "
            "exactly what was covered. If searched_whole_topic is false, no "
            f"match does not mean the key is absent. {payload_note}"
        ),
        annotations=READ_ONLY,
    )
    def find_messages_by_key(
        topic_name: TopicName,
        key: Annotated[
            str,
            Field(
                description="Exact message key.",
                min_length=1,
                max_length=MAX_SEARCH_KEY_BYTES,
            ),
        ],
        limit: Annotated[
            int, Field(description="Maximum matches to return.", ge=1, le=max_limit)
        ] = default_limit,
        partition: Annotated[
            int | None, Field(description="Only search this partition.", ge=0)
        ] = None,
        *,
        ctx: Context,
    ) -> KeySearchResponse:
        with operation_boundary("find_messages_by_key", ToolError, ctx.request_id):
            result = find_by_key_use_case.execute(
                topic_name, key, limit, partition=partition
            )
        return KeySearchResponse(
            topic=result.topic,
            match_count=len(result.matches),
            scanned=result.scanned,
            searched_whole_topic=result.searched_whole_topic,
            per_partition_limit=result.per_partition_limit,
            complete=result.complete,
            payloads_included=payloads_enabled,
            coverage=[
                ScanCoverageResponse(
                    partition=c.partition,
                    from_offset=c.from_offset,
                    to_offset=c.to_offset,
                    scanned_count=c.scanned_count,
                    reaches_start=c.reaches_start,
                )
                for c in result.coverage
            ],
            notes=list(result.notes),
            matches=[MessageResponse.from_domain(m) for m in result.matches],
        )

    @server.tool(
        name="list_dlq_topics",
        title="List dead letter queue topics",
        description=(
            "List existing topics configured as dead letter queues "
            "(STREAMSAGE_DLQ_TOPICS: exact names or glob patterns) with their "
            "approximate message count. StreamSage assumes no DLQ naming "
            "convention. Use inspect_messages on a DLQ topic to see its newest "
            "messages and headers."
        ),
        annotations=READ_ONLY,
    )
    def list_dlq_topics(*, ctx: Context) -> DlqOverviewResponse:
        with operation_boundary("list_dlq_topics", ToolError, ctx.request_id):
            overview = list_dlq_use_case.execute()
        return DlqOverviewResponse(
            configured=overview.configured,
            patterns=list(overview.patterns),
            topics=[
                DlqTopicResponse(
                    name=t.name,
                    partition_count=t.partition_count,
                    message_count=t.message_count,
                )
                for t in overview.topics
            ],
            notes=list(overview.notes),
        )
