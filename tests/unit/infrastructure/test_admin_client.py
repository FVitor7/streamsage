import logging
from typing import Any

import pytest

from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka import admin_client as admin_client_module
from streamsage.infrastructure.kafka.admin_client import (
    KafkaAdminClientFactory,
    build_admin_config,
)
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


def test_sasl_ssl_config() -> None:
    password = random_credential()
    config = build_admin_config(
        Settings(
            kafka_bootstrap_servers="kafka.example.com:9094",
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="SCRAM-SHA-256",
            kafka_sasl_username="test-user",
            kafka_sasl_password=password,  # type: ignore[arg-type]
            kafka_ssl_ca_location="/etc/ssl/ca.pem",
        )
    )
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "SCRAM-SHA-256"
    assert config["sasl.username"] == "test-user"
    assert config["sasl.password"] == password
    assert config["ssl.ca.location"] == "/etc/ssl/ca.pem"


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
    client = KafkaAdminClientFactory(settings).create()

    assert isinstance(client, RecordingAdminClient)
    assert captured["conf"] == build_admin_config(settings)
    assert isinstance(captured["kwargs"]["logger"], logging.Logger)
