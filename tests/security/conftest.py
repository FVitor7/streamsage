"""Fixtures for the `security` suite: a real SASL/SCRAM + TLS broker.

Start the broker with ``docker/kafka-secure/setup.sh`` and run the suite
with ``uv run pytest -m security``. Credentials and the test CA are read
from docker/kafka-secure/.generated/, which setup.sh creates (never in Git).
"""

import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from streamsage.domain.exceptions import KafkaConnectionError
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.admin_client import KafkaAdminClientFactory
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway
from streamsage.infrastructure.kafka.kafka_cluster_adapter import KafkaClusterAdapter

GENERATED = Path(__file__).resolve().parents[2] / "docker/kafka-secure/.generated"
SASL_PLAINTEXT_BOOTSTRAP = "localhost:9094"
SASL_SSL_BOOTSTRAP = "localhost:9095"
SCRAM_USER = "streamsage"
READY_TIMEOUT_SECONDS = 60
# Failure scenarios wait for the whole request timeout, so keep it short.
FAILURE_TIMEOUT_SECONDS = 3.0


@contextmanager
def cluster_adapter(settings: Settings) -> Iterator[KafkaClusterAdapter]:
    """Wire a real client exactly like the composition root does."""
    events = KafkaErrorEvents()
    with KafkaAdminClientFactory(settings).create(events) as client:
        yield KafkaClusterAdapter(
            KafkaAdminGateway(
                client,
                timeout_seconds=settings.kafka_request_timeout,
                error_events=events,
            )
        )


def scram_settings(
    *,
    bootstrap: str,
    protocol: str,
    password: str,
    ca_location: str | None = None,
    timeout: float = 10,
) -> Settings:
    return Settings(
        kafka_bootstrap_servers=bootstrap,
        kafka_security_protocol=protocol,  # type: ignore[arg-type]
        kafka_sasl_mechanism="SCRAM-SHA-512",
        kafka_sasl_username=SCRAM_USER,
        kafka_sasl_password=password,  # type: ignore[arg-type]
        kafka_ssl_ca_location=ca_location,
        kafka_request_timeout=timeout,
        kafka_socket_timeout=timeout,
    )


@pytest.fixture(scope="session")
def sasl_password() -> str:
    path = GENERATED / "sasl_password"
    if not path.is_file():
        pytest.fail(
            f"{path} not found. Start the secured broker with "
            "`docker/kafka-secure/setup.sh`."
        )
    return path.read_text().strip()


@pytest.fixture(scope="session")
def ca_location() -> str:
    return str(GENERATED / "ca.pem")


@pytest.fixture(scope="session", autouse=True)
def secured_broker_ready(sasl_password: str, ca_location: str) -> None:
    """Wait until the secured broker accepts the SCRAM user over TLS."""
    settings = scram_settings(
        bootstrap=SASL_SSL_BOOTSTRAP,
        protocol="SASL_SSL",
        password=sasl_password,
        ca_location=ca_location,
        timeout=5,
    )
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    last_error = "no response"
    with cluster_adapter(settings) as adapter:
        while time.monotonic() < deadline:
            try:
                adapter.get_cluster_info()
                return
            except KafkaConnectionError as exc:
                last_error = str(exc)
            time.sleep(1)
    pytest.fail(
        f"Secured Kafka not ready after {READY_TIMEOUT_SECONDS}s ({last_error}). "
        "Run `docker/kafka-secure/setup.sh`."
    )
