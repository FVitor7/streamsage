"""Composition root: wires settings, Kafka adapters, use cases and the MCP server."""

import argparse
import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import NoReturn

from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError
from pydantic_settings import SettingsError

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
from streamsage.application.use_cases.find_messages_by_key import (
    FindMessagesByKeyUseCase,
    KeySearchLimits,
)
from streamsage.application.use_cases.get_cluster_info import GetClusterInfoUseCase
from streamsage.application.use_cases.get_consumer_lag import GetConsumerLagUseCase
from streamsage.application.use_cases.inspect_messages import InspectMessagesUseCase
from streamsage.application.use_cases.list_consumer_groups import (
    ListConsumerGroupsUseCase,
)
from streamsage.application.use_cases.list_dlq_topics import ListDlqTopicsUseCase
from streamsage.application.use_cases.list_topics import ListTopicsUseCase
from streamsage.domain.diagnostics.catalog import severity_policy
from streamsage.domain.diagnostics.consumer_health import LagThresholds
from streamsage.domain.exceptions import ConfigurationError
from streamsage.domain.messages.dlq import DlqTopicMatcher
from streamsage.domain.messages.sanitizer import (
    DEFAULT_SENSITIVE_TERMS,
    SanitizationPolicy,
)
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.admin_client import KafkaAdminClientFactory
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.consumer import KafkaConsumerFactory
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway
from streamsage.infrastructure.kafka.kafka_cluster_adapter import KafkaClusterAdapter
from streamsage.infrastructure.kafka.kafka_consumer_group_adapter import (
    KafkaConsumerGroupAdapter,
)
from streamsage.infrastructure.kafka.kafka_message_adapter import KafkaMessageAdapter
from streamsage.infrastructure.kafka.kafka_topic_adapter import KafkaTopicAdapter
from streamsage.presentation.http.bearer_auth import generate_token, hash_token
from streamsage.presentation.mcp.authorization import ALL_SCOPES
from streamsage.presentation.mcp.server import create_server
from streamsage.shared.logging import configure_logging
from streamsage.shared.metrics import ServerMetrics

logger = logging.getLogger("streamsage")

EXIT_CONFIG_ERROR = 2


@dataclass(frozen=True, slots=True)
class Application:
    server: MCPServer
    metrics: ServerMetrics


@contextmanager
def build_application(settings: Settings) -> Iterator[Application]:
    """Yield a fully wired application; the Kafka client is closed on exit."""
    metrics = ServerMetrics(version=__version__)
    error_events = KafkaErrorEvents()
    with KafkaAdminClientFactory(settings).create(error_events) as admin_client:
        gateway = KafkaAdminGateway(
            admin_client,
            timeout_seconds=settings.kafka_request_timeout,
            error_events=error_events,
            metrics=metrics,
        )
        topic_reader = KafkaTopicAdapter(gateway)
        cluster_reader = KafkaClusterAdapter(gateway)
        group_reader = KafkaConsumerGroupAdapter(gateway)
        policy = severity_policy(settings.finding_severity_overrides)
        message_reader = KafkaMessageAdapter(
            gateway,
            KafkaConsumerFactory(settings),
            timeout_seconds=settings.kafka_request_timeout,
            metrics=metrics,
            max_value_bytes=settings.message_max_decode_bytes,
        )
        sanitization = SanitizationPolicy(
            payloads_enabled=settings.message_payloads_enabled,
            keys_enabled=settings.message_keys_enabled,
            max_payload_bytes=settings.message_max_payload_bytes,
            max_decode_bytes=settings.message_max_decode_bytes,
            sensitive_terms=DEFAULT_SENSITIVE_TERMS | settings.extra_sensitive_fields,
        )
        server = create_server(
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
            inspect_messages_use_case=InspectMessagesUseCase(
                message_reader,
                sanitization,
                settings.message_max_limit,
                max_read=settings.message_inspect_max_read,
            ),
            find_messages_by_key_use_case=FindMessagesByKeyUseCase(
                message_reader,
                sanitization,
                KeySearchLimits(
                    max_results=settings.message_max_limit,
                    max_scan_per_partition=settings.message_key_search_max_per_partition,
                    max_scan_total=settings.message_key_search_max_total,
                ),
            ),
            list_dlq_topics_use_case=ListDlqTopicsUseCase(
                topic_reader,
                message_reader,
                DlqTopicMatcher(settings.dlq_topic_patterns),
            ),
            message_default_limit=settings.message_default_limit,
            message_max_limit=settings.message_max_limit,
            message_payloads_enabled=settings.message_payloads_enabled,
            include_internal_topics=settings.include_internal_topics,
            metrics=metrics,
        )
        yield Application(server=server, metrics=metrics)


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


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.command == "generate-token":
        _print_new_token(args.name, args.scope or [])
        return
    serve()


def serve() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    if settings.uses_unencrypted_remote_connection:
        logger.warning(
            "Kafka traffic to a non-local broker is not encrypted; "
            "PLAINTEXT/SASL_PLAINTEXT are intended for local development only."
        )
    if settings.transport == "http" and not settings.http_is_loopback:
        logger.warning(
            "The HTTP transport is listening on a non-loopback address without "
            "TLS; put a TLS-terminating reverse proxy in front of it."
        )
    logger.info(
        "starting streamsage",
        extra={
            "event": {
                "transport": settings.transport,
                "security_protocol": settings.kafka_security_protocol,
            }
        },
    )
    try:
        with build_application(settings) as app:
            if settings.transport == "http":
                _run_http(app, settings)
            else:
                app.server.run("stdio")
    except ConfigurationError as exc:
        _exit_with_config_error(str(exc))
    logger.info("streamsage stopped")


def _run_http(app: Application, settings: Settings) -> None:
    import uvicorn

    from streamsage.presentation.http.app import MCP_PATH, build_http_app

    logger.info(
        "serving streamable http",
        extra={
            "event": {
                "host": settings.http_host,
                "port": settings.http_port,
                "path": MCP_PATH,
                "tokens": len(settings.http_tokens),
            }
        },
    )
    config = uvicorn.Config(
        build_http_app(app.server, settings, app.metrics),
        host=settings.http_host,
        port=settings.http_port,
        log_config=None,  # keep StreamSage's JSON logging on stderr
        lifespan="on",
        server_header=False,
    )
    uvicorn.Server(config).run()


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="streamsage",
        description="Read-only MCP server for Apache Kafka.",
    )
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("serve", help="run the MCP server (default)")
    token = commands.add_parser(
        "generate-token", help="create a bearer token for the HTTP transport"
    )
    token.add_argument("--name", required=True, help="identifies the token in logs")
    token.add_argument(
        "--scope",
        action="append",
        choices=sorted(ALL_SCOPES),
        help="grant a scope (repeatable; default: kafka:metadata)",
    )
    return parser.parse_args(argv)


def _print_new_token(name: str, scopes: list[str]) -> None:
    token = generate_token()
    entry = {
        "name": name,
        "sha256": hash_token(token),
        "scopes": sorted(set(scopes or ["kafka:metadata"])),
    }
    print(f"Token (give it to the MCP client; it is not stored anywhere):\n  {token}")
    print("\nAdd this entry to the STREAMSAGE_HTTP_TOKENS JSON list:")
    print(f"  {json.dumps(entry)}")


if __name__ == "__main__":
    main()
