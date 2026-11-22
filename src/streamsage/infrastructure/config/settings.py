"""Server configuration loaded from environment variables (prefix ``STREAMSAGE_``).

Credentials are configured once, by the operator, through the environment.
MCP clients can never supply or override Kafka credentials per tool call.
"""

from pathlib import Path
from typing import Literal, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from streamsage.domain.diagnostics.catalog import ALL_FINDING_CODES
from streamsage.domain.diagnostics.severity import Severity

SecurityProtocol = Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
# Mechanisms that only need username/password. GSSAPI and OAUTHBEARER need
# additional configuration and are not supported yet.
SaslMechanism = Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

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
    include_internal_topics: bool = False

    # Per-partition lag, in messages, used by analyze_consumer_health.
    # Starting points only: tune them to each consumer's normal throughput.
    lag_warning_threshold: int = Field(default=1_000, ge=1)
    lag_critical_threshold: int = Field(default=10_000, ge=1)

    # Severity per finding code, as JSON, e.g. '{"NO_ACTIVE_MEMBERS": "info"}'.
    finding_severity_overrides: dict[str, Severity] = Field(default_factory=dict)

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
