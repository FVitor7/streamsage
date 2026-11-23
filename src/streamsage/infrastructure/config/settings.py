"""Server configuration loaded from environment variables (prefix ``STREAMSAGE_``).

Credentials are configured once, by the operator, through the environment.
MCP clients can never supply or override Kafka credentials per tool call.
"""

from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from streamsage.domain.diagnostics.catalog import ALL_FINDING_CODES
from streamsage.domain.diagnostics.severity import Severity
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.messages.dlq import DlqTopicMatcher

SecurityProtocol = Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
# Mechanisms that only need username/password. GSSAPI and OAUTHBEARER need
# additional configuration and are not supported yet.
SaslMechanism = Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
Transport = Literal["stdio", "http"]
# Must match streamsage.presentation.mcp.authorization (checked by a test).
HttpScope = Literal["kafka:metadata", "kafka:messages"]


class HttpTokenSetting(BaseModel):
    """One accepted bearer token, identified by its SHA-256 hash."""

    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scopes: list[HttpScope] = Field(min_length=1)


_SASL_PROTOCOLS = frozenset({"SASL_PLAINTEXT", "SASL_SSL"})
_TLS_PROTOCOLS = frozenset({"SSL", "SASL_SSL"})
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "[::1]"})


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STREAMSAGE_",
        env_file=".env",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = Field(default="localhost:9092", min_length=1)

    kafka_security_protocol: SecurityProtocol = "PLAINTEXT"
    kafka_sasl_mechanism: SaslMechanism | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None
    kafka_ssl_ca_location: str | None = None

    # Seconds. Upper bound keeps a misconfiguration from hanging tool calls.
    kafka_request_timeout: float = Field(default=10, gt=0, le=300)
    kafka_socket_timeout: float = Field(default=10, gt=0, le=300)

    log_level: LogLevel = "INFO"

    # Transport. "http" serves Streamable HTTP and requires bearer tokens.
    transport: Transport = "stdio"
    http_host: str = Field(default="127.0.0.1", min_length=1)
    http_port: int = Field(default=8000, ge=1, le=65_535)
    # JSON list of {"name", "sha256", "scopes"}; create with
    # `streamsage generate-token`. Only hashes are configured, never tokens.
    http_tokens: list[HttpTokenSetting] = Field(default_factory=list)
    # /metrics requires a valid token unless this is true.
    http_metrics_public: bool = False
    include_internal_topics: bool = False

    # Per-partition offset lag (offsets behind, an upper bound on unconsumed
    # messages) used by analyze_consumer_health.
    # Starting points only: tune them to each consumer's normal throughput.
    lag_warning_threshold: int = Field(default=1_000, ge=1)
    lag_critical_threshold: int = Field(default=10_000, ge=1)

    # Severity per finding code, as JSON, e.g. '{"NO_ACTIVE_MEMBERS": "info"}'.
    finding_severity_overrides: dict[str, Severity] = Field(default_factory=dict)

    # Message inspection (M4). Payloads are omitted unless explicitly enabled.
    message_payloads_enabled: bool = False
    message_keys_enabled: bool = True
    message_max_payload_bytes: int = Field(default=1024, ge=1, le=65_536)
    message_default_limit: int = Field(default=20, ge=1)
    message_max_limit: int = Field(default=100, ge=1, le=1_000)
    # Messages read at most per inspect_messages call (across partitions);
    # beyond it the result is marked as not exact.
    message_inspect_max_read: int = Field(default=10_000, ge=1, le=1_000_000)
    # Payloads above this size are neither kept nor decoded (size only).
    message_max_decode_bytes: int = Field(default=262_144, ge=1, le=16_777_216)
    # Extra field/header names to redact, comma-separated (added to defaults).
    message_sensitive_fields: str = ""
    # Key search scans at most this many of the newest messages.
    message_key_search_max_per_partition: int = Field(default=1_000, ge=1, le=100_000)
    message_key_search_max_total: int = Field(default=10_000, ge=1, le=1_000_000)

    # DLQ topics: exact names and/or glob patterns, comma-separated.
    dlq_topics: str = ""

    @field_validator(
        "kafka_sasl_mechanism",
        "kafka_sasl_username",
        "kafka_sasl_password",
        "kafka_ssl_ca_location",
        mode="before",
    )
    @classmethod
    def _blank_as_none(cls, value: object) -> object:
        # `.env.example` ships keys with empty values; treat them as unset.
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("finding_severity_overrides")
    @classmethod
    def _validate_finding_codes(cls, value: dict[str, Severity]) -> dict[str, Severity]:
        unknown = sorted(set(value) - ALL_FINDING_CODES)
        if unknown:
            raise ValueError(
                "unknown finding code(s): "
                + ", ".join(unknown)
                + "; valid codes: "
                + ", ".join(sorted(ALL_FINDING_CODES))
            )
        return value

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def _validate_bootstrap_servers(cls, value: str) -> str:
        servers = [server.strip() for server in value.split(",")]
        if any(not server for server in servers):
            raise ValueError("must be a comma-separated list of host:port entries")
        for server in servers:
            host, sep, port = server.rpartition(":")
            if not sep or not host or not port.isdigit():
                raise ValueError(f"invalid bootstrap server entry {server!r}")
        return ",".join(servers)

    @model_validator(mode="after")
    def _validate_security(self) -> Self:
        uses_sasl = self.kafka_security_protocol in _SASL_PROTOCOLS
        sasl_fields = {
            "kafka_sasl_mechanism": self.kafka_sasl_mechanism,
            "kafka_sasl_username": self.kafka_sasl_username,
            "kafka_sasl_password": self.kafka_sasl_password,
        }
        if uses_sasl:
            missing = [name for name, value in sasl_fields.items() if value is None]
            if missing:
                raise ValueError(
                    f"{self.kafka_security_protocol} requires: "
                    + ", ".join(f"STREAMSAGE_{name.upper()}" for name in missing)
                )
        else:
            provided = [name for name, value in sasl_fields.items() if value]
            if provided:
                raise ValueError(
                    "SASL settings are set but kafka_security_protocol is "
                    f"{self.kafka_security_protocol}; use SASL_SSL or "
                    "SASL_PLAINTEXT, or unset: "
                    + ", ".join(f"STREAMSAGE_{name.upper()}" for name in provided)
                )
        if (
            self.kafka_ssl_ca_location
            and self.kafka_security_protocol not in _TLS_PROTOCOLS
        ):
            raise ValueError(
                "STREAMSAGE_KAFKA_SSL_CA_LOCATION requires kafka_security_protocol "
                "SSL or SASL_SSL"
            )
        if (
            self.kafka_ssl_ca_location
            and not Path(self.kafka_ssl_ca_location).is_file()
        ):
            raise ValueError("STREAMSAGE_KAFKA_SSL_CA_LOCATION is not a readable file")
        try:
            DlqTopicMatcher(self.dlq_topic_patterns)
        except InvalidInputError as exc:
            raise ValueError(f"STREAMSAGE_DLQ_TOPICS: {exc}") from None
        if self.transport == "http" and not self.http_tokens:
            raise ValueError(
                "STREAMSAGE_TRANSPORT=http requires at least one token in "
                "STREAMSAGE_HTTP_TOKENS (create one with `streamsage generate-token`)"
            )
        names = [token.name for token in self.http_tokens]
        if len(names) != len(set(names)):
            raise ValueError("STREAMSAGE_HTTP_TOKENS names must be unique")
        if self.message_max_decode_bytes < self.message_max_payload_bytes:
            raise ValueError(
                "STREAMSAGE_MESSAGE_MAX_DECODE_BYTES must be at least "
                "STREAMSAGE_MESSAGE_MAX_PAYLOAD_BYTES"
            )
        if self.message_inspect_max_read < self.message_max_limit:
            raise ValueError(
                "STREAMSAGE_MESSAGE_INSPECT_MAX_READ must be at least "
                "STREAMSAGE_MESSAGE_MAX_LIMIT"
            )
        if self.message_default_limit > self.message_max_limit:
            raise ValueError(
                "STREAMSAGE_MESSAGE_DEFAULT_LIMIT must not exceed "
                "STREAMSAGE_MESSAGE_MAX_LIMIT"
            )
        if self.lag_critical_threshold < self.lag_warning_threshold:
            raise ValueError(
                "STREAMSAGE_LAG_CRITICAL_THRESHOLD must be greater than or equal "
                "to STREAMSAGE_LAG_WARNING_THRESHOLD"
            )
        return self

    @property
    def uses_unencrypted_remote_connection(self) -> bool:
        """True when traffic to a non-loopback broker would not use TLS."""
        if self.kafka_security_protocol in _TLS_PROTOCOLS:
            return False
        hosts = {
            server.rpartition(":")[0].lower()
            for server in self.kafka_bootstrap_servers.split(",")
        }
        return not hosts <= _LOOPBACK_HOSTS

    @property
    def extra_sensitive_fields(self) -> frozenset[str]:
        return frozenset(
            name.strip()
            for name in self.message_sensitive_fields.split(",")
            if name.strip()
        )

    @property
    def dlq_topic_patterns(self) -> tuple[str, ...]:
        return tuple(p.strip() for p in self.dlq_topics.split(",") if p.strip())

    @property
    def http_is_loopback(self) -> bool:
        return self.http_host.lower() in _LOOPBACK_HOSTS
