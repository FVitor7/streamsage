import pytest
from pydantic import ValidationError

from streamsage.infrastructure.config.settings import Settings
from tests.fakes import random_credential


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    """Ignore the developer's real environment and .env file."""
    import os

    for key in list(os.environ):
        if key.startswith("STREAMSAGE_"):
            monkeypatch.delenv(key)
    monkeypatch.chdir(tmp_path)  # type: ignore[arg-type]


def test_defaults() -> None:
    settings = Settings()
    assert settings.kafka_bootstrap_servers == "localhost:9092"
    assert settings.kafka_security_protocol == "PLAINTEXT"
    assert settings.kafka_request_timeout == 10
    assert settings.include_internal_topics is False
    assert settings.uses_unencrypted_remote_connection is False


def test_reads_prefixed_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS", "k1:9092, k2:9093")
    monkeypatch.setenv("STREAMSAGE_KAFKA_REQUEST_TIMEOUT", "2.5")
    monkeypatch.setenv("STREAMSAGE_INCLUDE_INTERNAL_TOPICS", "true")
    settings = Settings()
    assert settings.kafka_bootstrap_servers == "k1:9092,k2:9093"
    assert settings.kafka_request_timeout == 2.5
    assert settings.include_internal_topics is True


def test_blank_optional_values_are_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    # Mirrors .env.example, which ships empty SASL keys.
    for key in ("SASL_MECHANISM", "SASL_USERNAME", "SASL_PASSWORD", "SSL_CA_LOCATION"):
        monkeypatch.setenv(f"STREAMSAGE_KAFKA_{key}", "")
    settings = Settings()
    assert settings.kafka_sasl_mechanism is None
    assert settings.kafka_sasl_password is None


def test_sasl_settings_are_accepted_and_password_is_secret() -> None:
    password = random_credential()
    settings = Settings(
        kafka_bootstrap_servers="kafka.example.com:9094",
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="SCRAM-SHA-512",
        kafka_sasl_username="test-user",
        kafka_sasl_password=password,  # type: ignore[arg-type]
    )
    assert settings.kafka_sasl_password is not None
    assert settings.kafka_sasl_password.get_secret_value() == password
    assert password not in repr(settings)
    assert password not in str(settings.model_dump())
    assert settings.uses_unencrypted_remote_connection is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"kafka_security_protocol": "SASL_SSL"},
        {
            "kafka_security_protocol": "SASL_PLAINTEXT",
            "kafka_sasl_mechanism": "PLAIN",
            "kafka_sasl_username": "u",
        },
    ],
)
def test_sasl_protocol_requires_credentials(overrides: dict[str, str]) -> None:
    with pytest.raises(ValidationError, match="requires"):
        Settings(**overrides)  # type: ignore[arg-type]


def test_credentials_without_sasl_protocol_are_rejected() -> None:
    with pytest.raises(ValidationError, match="SASL settings are set"):
        Settings(kafka_sasl_username="u")


def test_ca_location_requires_tls_protocol() -> None:
    with pytest.raises(ValidationError, match="SSL_CA_LOCATION"):
        Settings(kafka_ssl_ca_location="/etc/ca.pem")
    assert (
        Settings(
            kafka_security_protocol="SSL", kafka_ssl_ca_location="/etc/ca.pem"
        ).kafka_ssl_ca_location
        == "/etc/ca.pem"
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("kafka_security_protocol", "TLS"),
        ("kafka_sasl_mechanism", "GSSAPI"),
        ("kafka_request_timeout", 0),
        ("kafka_socket_timeout", -1),
        ("kafka_request_timeout", 301),
        ("log_level", "VERBOSE"),
        ("kafka_bootstrap_servers", ""),
        ("kafka_bootstrap_servers", "localhost"),
        ("kafka_bootstrap_servers", "localhost:abc"),
        ("kafka_bootstrap_servers", "k1:9092,,k2:9092"),
    ],
)
def test_invalid_values_are_rejected(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        Settings(**{field: value})  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "servers,protocol,expected",
    [
        ("localhost:9092", "PLAINTEXT", False),
        ("127.0.0.1:9092,[::1]:9092", "PLAINTEXT", False),
        ("kafka.internal:9092", "PLAINTEXT", True),
        ("localhost:9092,kafka.internal:9092", "PLAINTEXT", True),
        ("kafka.internal:9093", "SSL", False),
    ],
)
def test_unencrypted_remote_connection_detection(
    servers: str, protocol: str, expected: bool
) -> None:
    settings = Settings(
        kafka_bootstrap_servers=servers,
        kafka_security_protocol=protocol,  # type: ignore[arg-type]
    )
    assert settings.uses_unencrypted_remote_connection is expected
