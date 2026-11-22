import logging
import time
from collections.abc import Sequence

from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicCollection,
    TopicPartition,
)
from confluent_kafka.admin._topic import TopicDescription

from streamsage.domain.entities.message import (
    MessageReadResult,
    OffsetRange,
    PartitionOffsets,
    RawHeader,
    RawMessage,
    TimestampType,
)
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.consumer import KafkaConsumerFactory
from streamsage.infrastructure.kafka.errors import translate_kafka_exception
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 0.5


class KafkaMessageAdapter:
    """``MessageReader`` using a short-lived, group-less consumer per call.

    Never logs message keys, headers or payloads.
    """

    def __init__(
        self,
        gateway: KafkaAdminGateway,
        consumer_factory: KafkaConsumerFactory,
        timeout_seconds: float,
    ) -> None:
        self._gateway = gateway
        self._consumer_factory = consumer_factory
        self._timeout_seconds = timeout_seconds

    def partition_offsets(self, topic: str) -> list[PartitionOffsets]:
        # DescribeTopics never auto-creates the topic (see KafkaTopicAdapter).
        description: TopicDescription = self._gateway.request(
            "topic description",
            lambda admin, timeout: admin.describe_topics(
                TopicCollection([topic]), request_timeout=timeout
            )[topic],
            not_found_message=f"Topic '{topic}' does not exist.",
        )
        partitions = sorted(p.id for p in description.partitions)
        events = KafkaErrorEvents()
        consumer = self._consumer_factory.create(events)
        try:
            offsets = []
            for partition in partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=self._timeout_seconds
                )
                offsets.append(PartitionOffsets(partition, low, high))
            return offsets
        except KafkaException as exc:
            raise self._failure(exc, consumer, events, "offset lookup") from exc
        finally:
            consumer.close()

    def read_range(
        self, topic: str, ranges: Sequence[OffsetRange]
    ) -> MessageReadResult:
        wanted = {r.partition: r for r in ranges if r.size > 0}
        if not wanted:
            return MessageReadResult(messages=(), complete=True)
        events = KafkaErrorEvents()
        consumer = self._consumer_factory.create(events)
        try:
            return self._read(consumer, topic, wanted)
        except KafkaException as exc:
            raise self._failure(exc, consumer, events, "message read") from exc
        finally:
            consumer.close()

    def _read(
        self, consumer: Consumer, topic: str, wanted: dict[int, OffsetRange]
    ) -> MessageReadResult:
        consumer.assign(
            [TopicPartition(topic, p, r.start) for p, r in sorted(wanted.items())]
        )
        pending = set(wanted)
        messages: list[RawMessage] = []
        deadline = time.monotonic() + self._timeout_seconds
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            message = consumer.poll(min(_POLL_INTERVAL_SECONDS, remaining))
            if message is None:
                continue
            partition = message.partition()
            if partition is None or partition not in pending:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    pending.discard(partition)
                elif error.fatal():
                    raise KafkaException(error)
                continue
            offset = message.offset()
            stop = wanted[partition].stop
            if offset is None or offset >= stop:
                pending.discard(partition)
                continue
            messages.append(to_raw_message(message, topic))
            if offset >= stop - 1:
                pending.discard(partition)
        logger.info(
            "messages read",
            extra={
                "event": {
                    "partitions": len(wanted),
                    "messages": len(messages),
                    "complete": not pending,
                }
            },
        )
        return MessageReadResult(messages=tuple(messages), complete=not pending)

    def _failure(
        self,
        exc: KafkaException,
        consumer: Consumer,
        events: KafkaErrorEvents,
        operation: str,
    ) -> Exception:
        consumer.poll(0)  # deliver queued error events
        return translate_kafka_exception(
            exc,
            operation=operation,
            timeout_seconds=self._timeout_seconds,
            observed_codes=events.take_codes(),
        )


_TIMESTAMP_TYPES = {
    TIMESTAMP_CREATE_TIME: TimestampType.CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME: TimestampType.LOG_APPEND_TIME,
}


def to_raw_message(message: Message, topic: str) -> RawMessage:
    timestamp_type, timestamp = message.timestamp()
    kind = _TIMESTAMP_TYPES.get(timestamp_type, TimestampType.NOT_AVAILABLE)
    return RawMessage(
        topic=message.topic() or topic,
        partition=message.partition() or 0,
        offset=message.offset() or 0,
        timestamp_ms=timestamp if kind is not TimestampType.NOT_AVAILABLE else None,
        timestamp_type=kind,
        key=message.key(),
        value=message.value(),
        headers=tuple(_to_header(k, v) for k, v in _header_items(message)),
    )


def _header_items(message: Message) -> list[tuple[str, object]]:
    headers = message.headers()
    if not headers:
        return []
    items = headers.items() if isinstance(headers, dict) else headers
    return [(str(key), value) for key, value in items]


def _to_header(key: str, value: object) -> RawHeader:
    if value is None:
        return RawHeader(key, None)
    if isinstance(value, bytes):
        return RawHeader(key, value)
    return RawHeader(key, str(value).encode("utf-8"))
