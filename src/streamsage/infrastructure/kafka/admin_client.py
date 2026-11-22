"""Creation of the confluent-kafka AdminClient.

This is the only place that builds a Kafka client. The client is used solely
for metadata reads; StreamSage exposes no write operations.
"""

import logging

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from streamsage.domain.exceptions import ConfigurationError
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents

KafkaConfig = dict[str, str | int | float | bool]

CLIENT_ID = "streamsage"
_LIBRDKAFKA_LOGGER = "streamsage.kafka.librdkafka"


def build_admin_config(settings: Settings) -> KafkaConfig:
    """Translate settings into librdkafka configuration properties.

    The returned dict contains the SASL password when SASL is configured, so
    it must never be logged or returned to clients.
    """
    config: KafkaConfig = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": CLIENT_ID,
        "security.protocol": settings.kafka_security_protocol,
        "socket.timeout.ms": int(settings.kafka_socket_timeout * 1000),
    }
    if settings.kafka_sasl_mechanism is not None:
        config["sasl.mechanism"] = settings.kafka_sasl_mechanism
    if settings.kafka_sasl_username is not None:
        config["sasl.username"] = settings.kafka_sasl_username
    if settings.kafka_sasl_password is not None:
        config["sasl.password"] = settings.kafka_sasl_password.get_secret_value()
    if settings.kafka_ssl_ca_location is not None:
        config["ssl.ca.location"] = settings.kafka_ssl_ca_location
    return config


class KafkaAdminClientFactory:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def create(self, error_events: KafkaErrorEvents) -> AdminClient:
        """Create the client, reporting its error events to ``error_events``.

        Raises ``ConfigurationError`` if librdkafka rejects the configuration
        (for example an unreadable CA file).
        """
        try:
            # librdkafka logs are routed through Python logging (stderr),
            # keeping stdout free for the MCP stdio transport.
            return AdminClient(
                build_admin_config(self._settings),
                logger=logging.getLogger(_LIBRDKAFKA_LOGGER),
                error_cb=error_events.record,
            )
        except KafkaException as exc:
            code = exc.args[0].name() if exc.args else "unknown"
            raise ConfigurationError(
                f"The Kafka client rejected the configuration (code {code}). "
                "Check the STREAMSAGE_KAFKA_* security settings."
            ) from None
