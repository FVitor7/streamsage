import logging
import time
from collections import deque
from collections.abc import Callable, Sequence

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
    KeyScanResult,
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
from streamsage.infrastructure.kafka.gateway import (
    KafkaAdminGateway,
    KafkaMetrics,
    NoKafkaMetrics,
)

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
        metrics: KafkaMetrics | None = None,
    ) -> None:
        self._gateway = gateway
        self._consumer_factory = consumer_factory
        self._timeout_seconds = timeout_seconds
        self._metrics: KafkaMetrics = metrics or NoKafkaMetrics()

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
        with self._metrics.kafka_request("offset lookup"):
            return self._watermarks(topic, partitions)

    def _watermarks(self, topic: str, partitions: list[int]) -> list[PartitionOffsets]:
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
        with self._metrics.kafka_request("message read"):
            return self._read_ranges(topic, wanted)

    def _read_ranges(
        self, topic: str, wanted: dict[int, OffsetRange]
    ) -> MessageReadResult:
        events = KafkaErrorEvents()
        consumer = self._consumer_factory.create(events)
        try:
            return self._read(consumer, topic, wanted)
        except KafkaException as exc:
            raise self._failure(exc, consumer, events, "message read") from exc
        finally:
            consumer.close()

    def find_by_key(
        self,
        topic: str,
        ranges: Sequence[OffsetRange],
        key: bytes,
        max_matches_per_partition: int,
    ) -> KeyScanResult:
        wanted = {r.partition: r for r in ranges if r.size > 0}
        if not wanted:
            return KeyScanResult(matches=(), scanned=0, complete=True)
        newest: dict[int, deque[RawMessage]] = {
            p: deque(maxlen=max_matches_per_partition) for p in wanted
        }
        scanned = 0

        def keep_matches(message: Message) -> None:
            nonlocal scanned
            scanned += 1
            if message.key() == key:
                newest[message.partition() or 0].append(to_raw_message(message, topic))

        events = KafkaErrorEvents()
        consumer = self._consumer_factory.create(events)
        try:
            with self._metrics.kafka_request("key search"):
                complete = self._consume(consumer, topic, wanted, keep_matches)
        except KafkaException as exc:
            raise self._failure(exc, consumer, events, "key search") from exc
        finally:
            consumer.close()
        matches = tuple(m for partition in newest.values() for m in partition)
        logger.info(
            "key search finished",
            extra={
                "event": {
                    "partitions": len(wanted),
                    "scanned": scanned,
                    "matches": len(matches),
                    "complete": complete,
                }
            },
        )
        return KeyScanResult(matches=matches, scanned=scanned, complete=complete)

    def _read(
        self, consumer: Consumer, topic: str, wanted: dict[int, OffsetRange]
    ) -> MessageReadResult:
        messages: list[RawMessage] = []
        complete = self._consume(
            consumer, topic, wanted, lambda m: messages.append(to_raw_message(m, topic))
        )
        logger.info(
            "messages read",
            extra={
                "event": {
                    "partitions": len(wanted),
                    "messages": len(messages),
                    "complete": complete,
                }
            },
        )
        return MessageReadResult(messages=tuple(messages), complete=complete)

    def _consume(
        self,
        consumer: Consumer,
        topic: str,
        wanted: dict[int, OffsetRange],
        on_message: Callable[[Message], None],
    ) -> bool:
        """Deliver every message in the ranges to ``on_message``.

        Returns True if every range was fully read before the timeout.
        """
        consumer.assign(
            [TopicPartition(topic, p, r.start) for p, r in sorted(wanted.items())]
        )
        pending = set(wanted)
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
            on_message(message)
            if offset >= stop - 1:
                pending.discard(partition)
        return not pending

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
