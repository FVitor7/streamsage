"""Composition root: wires settings, Kafka adapters, use cases and the MCP server."""

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import NoReturn

from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError
from pydantic_settings import SettingsError

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
from streamsage.application.use_cases.list_consumer_groups import (
    ListConsumerGroupsUseCase,
)
from streamsage.application.use_cases.list_topics import ListTopicsUseCase
from streamsage.domain.diagnostics.catalog import severity_policy
from streamsage.domain.diagnostics.consumer_health import LagThresholds
from streamsage.domain.exceptions import ConfigurationError
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.admin_client import KafkaAdminClientFactory
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway
from streamsage.infrastructure.kafka.kafka_cluster_adapter import KafkaClusterAdapter
from streamsage.infrastructure.kafka.kafka_consumer_group_adapter import (
    KafkaConsumerGroupAdapter,
)
from streamsage.infrastructure.kafka.kafka_topic_adapter import KafkaTopicAdapter
from streamsage.presentation.mcp.server import create_server
from streamsage.shared.logging import configure_logging

logger = logging.getLogger("streamsage")

EXIT_CONFIG_ERROR = 2


@contextmanager
def build_application(settings: Settings) -> Iterator[MCPServer]:
    """Yield a fully wired server; the Kafka client is closed on exit."""
    error_events = KafkaErrorEvents()
    with KafkaAdminClientFactory(settings).create(error_events) as admin_client:
        gateway = KafkaAdminGateway(
            admin_client,
            timeout_seconds=settings.kafka_request_timeout,
            error_events=error_events,
        )
        topic_reader = KafkaTopicAdapter(gateway)
        cluster_reader = KafkaClusterAdapter(gateway)
        group_reader = KafkaConsumerGroupAdapter(gateway)
        policy = severity_policy(settings.finding_severity_overrides)
        yield create_server(
            get_cluster_info_use_case=GetClusterInfoUseCase(cluster_reader),
            list_topics_use_case=ListTopicsUseCase(topic_reader),
            describe_topic_use_case=DescribeTopicUseCase(topic_reader),
            list_consumer_groups_use_case=ListConsumerGroupsUseCase(group_reader),
            describe_consumer_group_use_case=DescribeConsumerGroupUseCase(group_reader),
            get_consumer_lag_use_case=GetConsumerLagUseCase(group_reader),
            analyze_consumer_health_use_case=AnalyzeConsumerHealthUseCase(
                group_reader,
                LagThresholds(
                    warning=settings.lag_warning_threshold,
                    critical=settings.lag_critical_threshold,
                ),
                policy,
            ),
            detect_stalled_partitions_use_case=DetectStalledPartitionsUseCase(
                group_reader, policy
            ),
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
    except SettingsError as exc:
        # Raised when an environment value cannot be parsed (for example
        # invalid JSON). The message names the field, never its value.
        _exit_with_config_error(str(exc))


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
