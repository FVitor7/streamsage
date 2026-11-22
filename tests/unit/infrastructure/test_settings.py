from pathlib import Path

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsError

from streamsage.domain.diagnostics.severity import Severity
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


def test_ca_location_requires_tls_protocol(tmp_path: Path) -> None:
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("test")
    with pytest.raises(ValidationError, match="SSL_CA_LOCATION"):
        Settings(kafka_ssl_ca_location=str(ca_file))
    settings = Settings(
        kafka_security_protocol="SSL", kafka_ssl_ca_location=str(ca_file)
    )
    assert settings.kafka_ssl_ca_location == str(ca_file)


def test_ca_location_must_be_an_existing_file(tmp_path: Path) -> None:
    for location in (tmp_path / "missing.pem", tmp_path):
        with pytest.raises(ValidationError, match="not a readable file"):
            Settings(kafka_security_protocol="SSL", kafka_ssl_ca_location=str(location))


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


def test_lag_threshold_defaults_and_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings()
    assert (settings.lag_warning_threshold, settings.lag_critical_threshold) == (
        1_000,
        10_000,
    )
    monkeypatch.setenv("STREAMSAGE_LAG_WARNING_THRESHOLD", "50")
    monkeypatch.setenv("STREAMSAGE_LAG_CRITICAL_THRESHOLD", "50")
    settings = Settings()
    assert (settings.lag_warning_threshold, settings.lag_critical_threshold) == (50, 50)


@pytest.mark.parametrize(("warning", "critical"), [(0, 10), (100, 99), (10, 0)])
def test_inconsistent_lag_thresholds_are_rejected(warning: int, critical: int) -> None:
    with pytest.raises(ValidationError):
        Settings(lag_warning_threshold=warning, lag_critical_threshold=critical)


def test_finding_severity_overrides_from_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "STREAMSAGE_FINDING_SEVERITY_OVERRIDES",
        '{"NO_ACTIVE_MEMBERS": "info", "PARTITIONS_NOT_ADVANCING": "critical"}',
    )
    overrides = Settings().finding_severity_overrides
    assert overrides == {
        "NO_ACTIVE_MEMBERS": Severity.INFO,
        "PARTITIONS_NOT_ADVANCING": Severity.CRITICAL,
    }
    assert Settings.model_fields["finding_severity_overrides"].default_factory


@pytest.mark.parametrize(
    ("raw", "error"),
    [
        ('{"NO_SUCH_CODE": "info"}', ValidationError),
        ('{"NO_ACTIVE_MEMBERS": "urgent"}', ValidationError),
        ("not json", SettingsError),
    ],
)
def test_invalid_finding_severity_overrides(
    monkeypatch: pytest.MonkeyPatch, raw: str, error: type[Exception]
) -> None:
    monkeypatch.setenv("STREAMSAGE_FINDING_SEVERITY_OVERRIDES", raw)
    with pytest.raises(error):
        Settings()


def test_message_inspection_defaults() -> None:
    settings = Settings()
    assert settings.message_payloads_enabled is False
    assert settings.message_max_payload_bytes == 1024
    assert (settings.message_default_limit, settings.message_max_limit) == (20, 100)
    assert settings.extra_sensitive_fields == frozenset()


def test_extra_sensitive_fields_are_parsed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STREAMSAGE_MESSAGE_SENSITIVE_FIELDS", " birth_date, ,rg ")
    assert Settings().extra_sensitive_fields == frozenset({"birth_date", "rg"})


@pytest.mark.parametrize(
    "overrides",
    [
        {"message_default_limit": 101},
        {"message_max_limit": 0},
        {"message_max_limit": 1001},
        {"message_max_payload_bytes": 0},
        {"message_max_payload_bytes": 65_537},
    ],
)
def test_invalid_message_limits(overrides: dict[str, int]) -> None:
    with pytest.raises(ValidationError):
        Settings(**overrides)  # type: ignore[arg-type]


def test_key_search_and_dlq_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings()
    assert settings.message_key_search_max_per_partition == 1_000
    assert settings.message_key_search_max_total == 10_000
    assert settings.dlq_topic_patterns == ()
    monkeypatch.setenv("STREAMSAGE_DLQ_TOPICS", " *.DLQ , dead-letter-*,")
    assert Settings().dlq_topic_patterns == ("*.DLQ", "dead-letter-*")


def test_invalid_dlq_pattern_is_rejected() -> None:
    with pytest.raises(ValidationError, match="STREAMSAGE_DLQ_TOPICS"):
        Settings(dlq_topics="orders dlq")


def test_stdio_is_the_default_transport() -> None:
    settings = Settings()
    assert settings.transport == "stdio"
    assert (settings.http_host, settings.http_port) == ("127.0.0.1", 8000)
    assert settings.http_is_loopback is True


def test_http_tokens_from_json(monkeypatch: pytest.MonkeyPatch) -> None:
    digest = "a" * 64
    monkeypatch.setenv("STREAMSAGE_TRANSPORT", "http")
    monkeypatch.setenv(
        "STREAMSAGE_HTTP_TOKENS",
        f'[{{"name": "ci", "sha256": "{digest}", "scopes": ["kafka:metadata"]}}]',
    )
    settings = Settings()
    assert settings.transport == "http"
    assert settings.http_tokens[0].sha256 == digest


@pytest.mark.parametrize(
    "tokens",
    [
        [{"name": "x", "sha256": "A" * 64, "scopes": ["kafka:metadata"]}],  # uppercase
        [{"name": "x", "sha256": "a" * 63, "scopes": ["kafka:metadata"]}],
        [{"name": "x", "sha256": "a" * 64, "scopes": []}],
        [{"name": "x", "sha256": "a" * 64, "scopes": ["kafka:admin"]}],
        [{"name": "bad name", "sha256": "a" * 64, "scopes": ["kafka:metadata"]}],
        [
            {"name": "x", "sha256": "a" * 64, "scopes": ["kafka:metadata"]},
            {"name": "x", "sha256": "b" * 64, "scopes": ["kafka:metadata"]},
        ],
    ],
)
def test_invalid_http_tokens(tokens: list[dict[str, object]]) -> None:
    with pytest.raises(ValidationError):
        Settings(transport="http", http_tokens=tokens)  # type: ignore[arg-type]


def test_http_transport_requires_tokens() -> None:
    with pytest.raises(ValidationError, match="requires at least one token"):
        Settings(transport="http")


def test_non_loopback_http_host_is_detected() -> None:
    assert Settings(http_host="0.0.0.0").http_is_loopback is False
