from typing import Annotated

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from streamsage.application.use_cases.describe_topic import DescribeTopicUseCase
from streamsage.application.use_cases.list_topics import ListTopicsUseCase
from streamsage.domain.value_objects.topic_name import (
    MAX_TOPIC_NAME_LENGTH,
    TOPIC_NAME_PATTERN,
)
from streamsage.presentation.mcp.errors import operation_boundary
from streamsage.presentation.mcp.schemas import (
    TopicDetailResponse,
    TopicListResponse,
)
from streamsage.presentation.mcp.tools.annotations import READ_ONLY


def register_topic_tools(
    server: MCPServer,
    list_topics_use_case: ListTopicsUseCase,
    describe_topic_use_case: DescribeTopicUseCase,
    *,
    include_internal_by_default: bool,
) -> None:
    @server.tool(
        name="list_topics",
        title="List Kafka topics",
        description=(
            "List Kafka topics sorted by name, with partition count and "
            "replication factor. Internal topics are hidden unless requested."
        ),
        annotations=READ_ONLY,
    )
    def list_topics(
        include_internal: Annotated[
            bool,
            Field(description="Include internal topics such as __consumer_offsets."),
        ] = include_internal_by_default,
    ) -> TopicListResponse:
        with operation_boundary("list_topics", ToolError):
            topics = list_topics_use_case.execute(include_internal=include_internal)
        return TopicListResponse.from_domain(topics, include_internal=include_internal)

    @server.tool(
        name="describe_topic",
        title="Describe a Kafka topic",
        description=(
            "Describe one Kafka topic: every partition with its leader, replicas "
            "and in-sync replicas (ISR), plus under-replicated and leaderless "
            "partition counts. Returns an error if the topic does not exist; "
            "never creates topics."
        ),
        annotations=READ_ONLY,
    )
    def describe_topic(
        topic_name: Annotated[
            str,
            Field(
                description="Exact name of an existing Kafka topic.",
                min_length=1,
                max_length=MAX_TOPIC_NAME_LENGTH,
                pattern=TOPIC_NAME_PATTERN,
            ),
        ],
    ) -> TopicDetailResponse:
        with operation_boundary("describe_topic", ToolError):
            topic = describe_topic_use_case.execute(topic_name)
        return TopicDetailResponse.from_domain(topic)
