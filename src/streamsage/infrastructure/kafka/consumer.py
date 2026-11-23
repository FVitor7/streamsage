"""Creation of the short-lived consumer used to read messages.

The consumer only ever uses ``assign()`` with explicit offsets. It never
subscribes, so it never joins a consumer group, and it never commits
offsets. ``group.id`` is set only because librdkafka requires it; the group
does not appear on the cluster (checked by the integration tests).
"""

import logging
from typing import Any

from confluent_kafka import Consumer, KafkaException

from streamsage.domain.exceptions import ConfigurationError
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.admin_client import build_admin_config
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents

INSPECTOR_GROUP_ID = "streamsage-inspector-readonly"
_LIBRDKAFKA_LOGGER = "streamsage.kafka.librdkafka"


def build_consumer_config(settings: Settings) -> dict[str, Any]:
    """librdkafka consumer properties. Contains secrets; never log it."""
    return {
        **build_admin_config(settings),
        "group.id": INSPECTOR_GROUP_ID,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "enable.partition.eof": True,
        # Ranges are computed from watermarks; if retention deletes messages
        # meanwhile, continue from the earliest available offset.
        "auto.offset.reset": "earliest",
        # Bound memory: only small, bounded ranges are ever read.
        "max.partition.fetch.bytes": 1024 * 1024,
        "fetch.max.bytes": 5 * 1024 * 1024,
        "queued.max.messages.kbytes": 8 * 1024,
    }


class KafkaConsumerFactory:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def create(self, error_events: KafkaErrorEvents) -> Consumer:
        config = build_consumer_config(self._settings)
        config["logger"] = logging.getLogger(_LIBRDKAFKA_LOGGER)
        config["error_cb"] = error_events.record
        try:
            return Consumer(config)
        except (KafkaException, ValueError) as exc:
            name = (
                exc.args[0].name()
                if isinstance(exc, KafkaException) and exc.args
                else type(exc).__name__
            )
            raise ConfigurationError(
                f"The Kafka consumer rejected the configuration ({name}). "
                "Check the STREAMSAGE_KAFKA_* settings."
            ) from None
