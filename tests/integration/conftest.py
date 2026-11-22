"""Fixtures for tests against a real Kafka broker (see docker-compose.yml).

Run with: ``uv run pytest -m integration``. The bootstrap address can be
overridden with STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS.
"""

import os
import time
from collections.abc import Iterator

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.admin_client import KafkaAdminClientFactory
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.metadata import KafkaMetadataSource

BOOTSTRAP = os.environ.get("STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
READY_TIMEOUT_SECONDS = 90
SAMPLE_TOPICS = {"orders.created": 3, "payments.processed": 2, "inventory.updated": 1}


@pytest.fixture(scope="session")
def kafka_settings() -> Settings:
    return Settings(kafka_bootstrap_servers=BOOTSTRAP, kafka_request_timeout=10)


@pytest.fixture(scope="session")
def error_events() -> KafkaErrorEvents:
    return KafkaErrorEvents()


@pytest.fixture(scope="session")
def admin_client(
    kafka_settings: Settings, error_events: KafkaErrorEvents
) -> Iterator[AdminClient]:
    with KafkaAdminClientFactory(kafka_settings).create(error_events) as client:
        _wait_until_ready(client)
        yield client


@pytest.fixture
def metadata_source(
    admin_client: AdminClient, kafka_settings: Settings, error_events: KafkaErrorEvents
) -> KafkaMetadataSource:
    return KafkaMetadataSource(
        admin_client,
        timeout_seconds=kafka_settings.kafka_request_timeout,
        error_events=error_events,
    )


def _wait_until_ready(client: AdminClient) -> None:
    """Wait for the broker and the sample topics created by kafka-init."""
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    last_error = "no response"
    while time.monotonic() < deadline:
        try:
            topics = client.list_topics(timeout=5).topics
        except KafkaException as exc:
            last_error = str(exc)
        else:
            missing = set(SAMPLE_TOPICS) - set(topics)
            if not missing:
                return
            last_error = f"sample topics missing: {sorted(missing)}"
        time.sleep(1)
    pytest.fail(
        f"Kafka at {BOOTSTRAP} not ready after {READY_TIMEOUT_SECONDS}s "
        f"({last_error}). Start it with `docker compose up -d`."
    )
