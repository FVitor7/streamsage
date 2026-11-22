"""Shared cluster metadata access for the Kafka adapters."""

import logging

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin._metadata import ClusterMetadata

from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.errors import translate_kafka_exception

logger = logging.getLogger(__name__)


class KafkaMetadataSource:
    """Fetches full cluster metadata with a bounded timeout.

    Metadata is always requested for *all* topics (no topic argument). A
    request scoped to a specific topic name can trigger topic auto-creation
    on brokers with ``auto.create.topics.enable=true``, so StreamSage never
    issues one.

    ``error_events`` must be the collector registered as the client's
    ``error_cb`` (see ``KafkaAdminClientFactory``).
    """

    def __init__(
        self,
        admin_client: AdminClient,
        timeout_seconds: float,
        error_events: KafkaErrorEvents,
    ) -> None:
        self._admin_client = admin_client
        self._timeout_seconds = timeout_seconds
        self._error_events = error_events

    def fetch(self, operation: str) -> ClusterMetadata:
        try:
            metadata = self._admin_client.list_topics(timeout=self._timeout_seconds)
        except KafkaException as exc:
            self._drain_client_events()
            translated = translate_kafka_exception(
                exc,
                operation=operation,
                timeout_seconds=self._timeout_seconds,
                observed_codes=self._error_events.take_codes(),
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
        self._drain_client_events()
        self._error_events.take_codes()  # discard events from before this success
        return metadata

    def _drain_client_events(self) -> None:
        # librdkafka queues logs and error events until the application polls.
        # Polling after every request delivers them (logs reach Python logging,
        # errors reach KafkaErrorEvents) and keeps the queue from growing while
        # a broker is unreachable.
        self._admin_client.poll(0)
