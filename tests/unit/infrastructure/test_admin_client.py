import logging
from pathlib import Path
from typing import Any

import pytest

from streamsage.domain.exceptions import ConfigurationError
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka import admin_client as admin_client_module
from streamsage.infrastructure.kafka.admin_client import (
    KafkaAdminClientFactory,
    build_admin_config,
)
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from tests.fakes import random_credential


def test_plaintext_config() -> None:
    config = build_admin_config(
        Settings(kafka_bootstrap_servers="localhost:9092", kafka_socket_timeout=2.5)
    )
    assert config == {
        "bootstrap.servers": "localhost:9092",
        "client.id": "streamsage",
        "security.protocol": "PLAINTEXT",
        "socket.timeout.ms": 2500,
    }


def test_sasl_ssl_config(tmp_path: Path) -> None:
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("test")
    password = random_credential()
    config = build_admin_config(
        Settings(
            kafka_bootstrap_servers="kafka.example.com:9094",
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="SCRAM-SHA-256",
            kafka_sasl_username="test-user",
            kafka_sasl_password=password,  # type: ignore[arg-type]
            kafka_ssl_ca_location=str(ca_file),
        )
    )
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "SCRAM-SHA-256"
    assert config["sasl.username"] == "test-user"
    assert config["sasl.password"] == password
    assert config["ssl.ca.location"] == str(ca_file)


def test_config_never_enables_writes_or_auto_creation_keys() -> None:
    config = build_admin_config(Settings())
    assert not any("transactional" in key or "acks" in key for key in config)


def test_factory_passes_config_and_stderr_logger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class RecordingAdminClient:
        def __init__(self, conf: dict[str, Any], **kwargs: Any) -> None:
            captured["conf"] = conf
            captured["kwargs"] = kwargs

    monkeypatch.setattr(admin_client_module, "AdminClient", RecordingAdminClient)
    settings = Settings()
    events = KafkaErrorEvents()
    client = KafkaAdminClientFactory(settings).create(events)

    assert isinstance(client, RecordingAdminClient)
    assert captured["conf"] == build_admin_config(settings)
    assert isinstance(captured["kwargs"]["logger"], logging.Logger)
    assert captured["kwargs"]["error_cb"] == events.record


def test_rejected_configuration_becomes_configuration_error(tmp_path: Path) -> None:
    # A CA "file" that exists but is not a certificate: librdkafka rejects it
    # when the client is created, before any network activity.
    bogus_ca = tmp_path / "ca.pem"
    bogus_ca.write_text("not a certificate")
    password = random_credential()
    settings = Settings(
        kafka_bootstrap_servers="localhost:9095",
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="SCRAM-SHA-512",
        kafka_sasl_username="test-user",
        kafka_sasl_password=password,  # type: ignore[arg-type]
        kafka_ssl_ca_location=str(bogus_ca),
    )
    with pytest.raises(ConfigurationError, match="rejected the configuration") as exc:
        KafkaAdminClientFactory(settings).create(KafkaErrorEvents())
    assert password not in str(exc.value)
    assert exc.value.__cause__ is None
