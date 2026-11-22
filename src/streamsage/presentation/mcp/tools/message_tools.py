from typing import Annotated

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from streamsage.application.use_cases.inspect_messages import InspectMessagesUseCase
from streamsage.domain.value_objects.topic_name import (
    MAX_TOPIC_NAME_LENGTH,
    TOPIC_NAME_PATTERN,
)
from streamsage.presentation.mcp.errors import operation_boundary
from streamsage.presentation.mcp.schemas import (
    MessageInspectionResponse,
    MessageResponse,
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
