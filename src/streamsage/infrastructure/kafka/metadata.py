"""Shared cluster metadata access for the Kafka adapters."""

import logging

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin._metadata import ClusterMetadata

from streamsage.infrastructure.kafka.errors import translate_kafka_exception

logger = logging.getLogger(__name__)


class KafkaMetadataSource:
    """Fetches full cluster metadata with a bounded timeout.

    Metadata is always requested for *all* topics (no topic argument). A
    request scoped to a specific topic name can trigger topic auto-creation
    on brokers with ``auto.create.topics.enable=true`` depending on client
    configuration, so StreamSage never issues one.
    """

    def __init__(self, admin_client: AdminClient, timeout_seconds: float) -> None:
        self._admin_client = admin_client
        self._timeout_seconds = timeout_seconds

    def fetch(self, operation: str) -> ClusterMetadata:
        try:
            return self._admin_client.list_topics(timeout=self._timeout_seconds)
        except KafkaException as exc:
            translated = translate_kafka_exception(
                exc, operation=operation, timeout_seconds=self._timeout_seconds
            )
            logger.warning(
                "kafka metadata request failed",
                extra={
                    "event": {
                        "operation": operation,
                        "error_type": type(translated).__name__,
                        "kafka_error": str(exc.args[0]) if exc.args else None,
                    }
                },
            )
            raise translated from exc
