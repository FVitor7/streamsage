"""Composition root: wires settings, Kafka adapters, use cases and the MCP server."""

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import NoReturn

from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from streamsage.application.use_cases.describe_topic import DescribeTopicUseCase
from streamsage.application.use_cases.get_cluster_info import GetClusterInfoUseCase
from streamsage.application.use_cases.list_topics import ListTopicsUseCase
from streamsage.domain.exceptions import ConfigurationError
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.admin_client import KafkaAdminClientFactory
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.kafka_cluster_adapter import KafkaClusterAdapter
from streamsage.infrastructure.kafka.kafka_topic_adapter import KafkaTopicAdapter
from streamsage.infrastructure.kafka.metadata import KafkaMetadataSource
from streamsage.presentation.mcp.server import create_server
from streamsage.shared.logging import configure_logging

logger = logging.getLogger("streamsage")

EXIT_CONFIG_ERROR = 2


@contextmanager
def build_application(settings: Settings) -> Iterator[MCPServer]:
    """Yield a fully wired server; the Kafka client is closed on exit."""
    error_events = KafkaErrorEvents()
    with KafkaAdminClientFactory(settings).create(error_events) as admin_client:
        metadata_source = KafkaMetadataSource(
            admin_client,
            timeout_seconds=settings.kafka_request_timeout,
            error_events=error_events,
        )
        topic_reader = KafkaTopicAdapter(metadata_source)
        cluster_reader = KafkaClusterAdapter(metadata_source)
        yield create_server(
            get_cluster_info_use_case=GetClusterInfoUseCase(cluster_reader),
            list_topics_use_case=ListTopicsUseCase(topic_reader),
            describe_topic_use_case=DescribeTopicUseCase(topic_reader),
            include_internal_topics=settings.include_internal_topics,
        )


def load_settings() -> Settings:
    """Load settings, exiting with a sanitized message when they are invalid."""
    try:
        return Settings()
    except ValidationError as exc:
        # include_input=False keeps configured values (possibly secrets) out
        # of the output.
        details = "; ".join(
            f"{'.'.join(str(p) for p in err['loc']) or 'settings'}: {err['msg']}"
            for err in exc.errors(include_input=False, include_url=False)
        )
        _exit_with_config_error(details)


def _exit_with_config_error(details: str) -> NoReturn:
    print(f"streamsage: invalid configuration: {details}", file=sys.stderr)
    raise SystemExit(EXIT_CONFIG_ERROR) from None


def main() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    if settings.uses_unencrypted_remote_connection:
        logger.warning(
            "Kafka traffic to a non-local broker is not encrypted; "
            "PLAINTEXT/SASL_PLAINTEXT are intended for local development only."
        )
    logger.info(
        "starting streamsage",
        extra={
            "event": {
                "transport": "stdio",
                "security_protocol": settings.kafka_security_protocol,
            }
        },
    )
    try:
        with build_application(settings) as server:
            server.run("stdio")
    except ConfigurationError as exc:
        _exit_with_config_error(str(exc))
    logger.info("streamsage stopped")


if __name__ == "__main__":
    main()
