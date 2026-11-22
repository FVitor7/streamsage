"""SASL/SCRAM and TLS against a real secured broker.

These tests exercise the exact production wiring (settings -> client factory
-> metadata source -> adapter) and check that failures are explained
precisely without ever exposing credentials.
"""

import io
import json
import logging
import time
from collections.abc import Iterator

import pytest

from streamsage.domain.exceptions import KafkaConnectionError
from streamsage.shared.logging import configure_logging
from tests.fakes import random_credential
from tests.security.conftest import (
    FAILURE_TIMEOUT_SECONDS,
    SASL_PLAINTEXT_BOOTSTRAP,
    SASL_SSL_BOOTSTRAP,
    cluster_adapter,
    scram_settings,
)

pytestmark = pytest.mark.security


@pytest.fixture
def captured_logs() -> Iterator[io.StringIO]:
    stream = io.StringIO()
    configure_logging("DEBUG", stream=stream)
    yield stream
    logging.getLogger().handlers.clear()


def test_scram_over_plaintext_listener(sasl_password: str) -> None:
    settings = scram_settings(
        bootstrap=SASL_PLAINTEXT_BOOTSTRAP,
        protocol="SASL_PLAINTEXT",
        password=sasl_password,
    )
    with cluster_adapter(settings) as adapter:
        cluster = adapter.get_cluster_info()
    assert cluster.cluster_id == "streamsage-local-secure-01"
    assert cluster.broker_count == 1


def test_scram_over_tls_with_private_ca(sasl_password: str, ca_location: str) -> None:
    settings = scram_settings(
        bootstrap=SASL_SSL_BOOTSTRAP,
        protocol="SASL_SSL",
        password=sasl_password,
        ca_location=ca_location,
    )
    with cluster_adapter(settings) as adapter:
        cluster = adapter.get_cluster_info()
    assert cluster.cluster_id == "streamsage-local-secure-01"


@pytest.mark.parametrize(
    ("bootstrap", "protocol", "use_ca"),
    [
        (SASL_PLAINTEXT_BOOTSTRAP, "SASL_PLAINTEXT", False),
        (SASL_SSL_BOOTSTRAP, "SASL_SSL", True),
    ],
)
def test_wrong_password_is_reported_as_authentication_failure(
    bootstrap: str,
    protocol: str,
    use_ca: bool,
    ca_location: str,
    sasl_password: str,
    captured_logs: io.StringIO,
) -> None:
    wrong_password = random_credential()
    settings = scram_settings(
        bootstrap=bootstrap,
        protocol=protocol,
        password=wrong_password,
        ca_location=ca_location if use_ca else None,
        timeout=FAILURE_TIMEOUT_SECONDS,
    )
    started = time.monotonic()
    with (
        cluster_adapter(settings) as adapter,
        pytest.raises(
            KafkaConnectionError, match="Authentication with the Kafka cluster failed"
        ) as raised,
    ):
        adapter.get_cluster_info()

    assert time.monotonic() - started < FAILURE_TIMEOUT_SECONDS + 3
    logs = captured_logs.getvalue()
    for secret in (wrong_password, sasl_password):
        assert secret not in str(raised.value)
        assert secret not in logs
    # librdkafka's own logs are delivered (drained via poll) as JSON lines.
    assert any(
        json.loads(line)["logger"] == "streamsage.kafka.librdkafka"
        for line in logs.splitlines()
    )


def test_untrusted_certificate_is_reported_as_tls_failure(
    sasl_password: str, captured_logs: io.StringIO
) -> None:
    # No CA configured: the system trust store does not know the test CA.
    settings = scram_settings(
        bootstrap=SASL_SSL_BOOTSTRAP,
        protocol="SASL_SSL",
        password=sasl_password,
        timeout=FAILURE_TIMEOUT_SECONDS,
    )
    with (
        cluster_adapter(settings) as adapter,
        pytest.raises(KafkaConnectionError, match="TLS handshake") as raised,
    ):
        adapter.get_cluster_info()

    assert sasl_password not in str(raised.value)
    assert sasl_password not in captured_logs.getvalue()


def test_tls_client_cannot_talk_to_plaintext_listener(
    sasl_password: str, ca_location: str
) -> None:
    settings = scram_settings(
        bootstrap=SASL_PLAINTEXT_BOOTSTRAP,
        protocol="SASL_SSL",
        password=sasl_password,
        ca_location=ca_location,
        timeout=FAILURE_TIMEOUT_SECONDS,
    )
    with cluster_adapter(settings) as adapter, pytest.raises(KafkaConnectionError):
        adapter.get_cluster_info()
