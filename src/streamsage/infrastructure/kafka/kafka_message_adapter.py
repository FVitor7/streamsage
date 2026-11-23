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
from confluent_kafka.admin._listoffsets import ListOffsetsResultInfo, OffsetSpec
from confluent_kafka.admin._topic import TopicDescription

from streamsage.domain.entities.message import (
    KeyScanResult,
    MessageReadResult,
    OffsetRange,
    PartitionOffsets,
    RawHeader,
    RawMessage,
    TimestampType,
    TopicOffsets,
)
from streamsage.domain.messages.selection import NewestMessages
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
DEFAULT_MAX_VALUE_BYTES = 256 * 1024


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
        max_value_bytes: int = DEFAULT_MAX_VALUE_BYTES,
    ) -> None:
        self._gateway = gateway
        self._consumer_factory = consumer_factory
        self._timeout_seconds = timeout_seconds
        self._metrics: KafkaMetrics = metrics or NoKafkaMetrics()
        # Payloads larger than this are not kept (only their size), which
        # bounds the memory held per retained message.
        self._max_value_bytes = max_value_bytes

    def partition_offsets(self, topic: str) -> TopicOffsets:
        # DescribeTopics never auto-creates the topic (see KafkaTopicAdapter).
        description: TopicDescription = self._gateway.request(
            "topic description",
            lambda admin, timeout: admin.describe_topics(
                TopicCollection([topic]), request_timeout=timeout
            )[topic],
            not_found_message=f"Topic '{topic}' does not exist.",
        )
        partitions = sorted(p.id for p in description.partitions)
        # Two batched ListOffsets requests (earliest and latest) instead of
        # one watermark request per partition.
        # OffsetSpec factories lack type annotations upstream.
        earliest_spec = OffsetSpec.earliest()  # type: ignore[no-untyped-call]
        latest_spec = OffsetSpec.latest()  # type: ignore[no-untyped-call]
        earliest = self._list_offsets(
            topic, partitions, earliest_spec, "earliest offset lookup"
        )
        latest = self._list_offsets(
            topic, partitions, latest_spec, "log end offset lookup"
        )
        available: list[PartitionOffsets] = []
        unavailable: list[int] = []
        for partition in partitions:
            low, high = earliest.get(partition), latest.get(partition)
            if low is None or high is None:
                unavailable.append(partition)
            else:
                available.append(PartitionOffsets(partition, low, high))
        return TopicOffsets(tuple(available), tuple(unavailable))

    def _list_offsets(
        self, topic: str, partitions: list[int], spec: OffsetSpec, operation: str
    ) -> dict[int, int | None]:
        if not partitions:
            return {}
        requested = {TopicPartition(topic, p): spec for p in partitions}
        results: dict[TopicPartition, ListOffsetsResultInfo | None] = (
            self._gateway.request_each(
                operation,
                lambda admin, timeout: admin.list_offsets(
                    requested, request_timeout=timeout
                ),
            )
        )
        return {
            tp.partition: (
                info.offset if info is not None and info.offset >= 0 else None
            )
            for tp, info in results.items()
        }

    def read_range(
        self, topic: str, ranges: Sequence[OffsetRange]
    ) -> MessageReadResult:
        wanted = {r.partition: r for r in ranges if r.size > 0}
        if not wanted:
            return MessageReadResult(messages=(), complete=True)
        with self._metrics.kafka_request("message read"):
            return self._read_ranges(topic, wanted)

    def read_newest(
        self, topic: str, ranges: Sequence[OffsetRange], limit: int
    ) -> MessageReadResult:
        wanted = {r.partition: r for r in ranges if r.size > 0}
        if not wanted:
            return MessageReadResult(messages=(), complete=True)
        newest = NewestMessages(limit)
        events = KafkaErrorEvents()
        consumer = self._consumer_factory.create(events)
        try:
            with self._metrics.kafka_request("message read"):
                complete = self._consume(
                    consumer,
                    topic,
                    wanted,
                    lambda m: newest.offer(self._to_raw(m, topic)),
                )
        except KafkaException as exc:
            raise self._failure(exc, consumer, events, "message read") from exc
        finally:
            consumer.close()
        logger.info(
            "newest messages read",
            extra={
                "event": {
                    "partitions": len(wanted),
                    "messages_read": newest.offered,
                    "kept": len(newest),
                    "complete": complete,
                }
            },
        )
        return MessageReadResult(
            messages=newest.newest_first(),
            complete=complete,
            messages_read=newest.offered,
        )

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
                newest[message.partition() or 0].append(self._to_raw(message, topic))

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
            consumer, topic, wanted, lambda m: messages.append(self._to_raw(m, topic))
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
        return MessageReadResult(
            messages=tuple(messages), complete=complete, messages_read=len(messages)
        )

    def _to_raw(self, message: Message, topic: str) -> RawMessage:
        return to_raw_message(message, topic, self._max_value_bytes)

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


def to_raw_message(
    message: Message, topic: str, max_value_bytes: int | None = None
) -> RawMessage:
    """Convert a consumer message, dropping payloads above ``max_value_bytes``.

    A dropped payload keeps its size (``original_value_size``), so it is
    reported as too large instead of being held in memory.
    """
    timestamp_type, timestamp = message.timestamp()
    kind = _TIMESTAMP_TYPES.get(timestamp_type, TimestampType.NOT_AVAILABLE)
    value = message.value()
    original_size = None
    if (
        value is not None
        and max_value_bytes is not None
        and len(value) > max_value_bytes
    ):
        original_size = len(value)
        value = b""
    return RawMessage(
        topic=message.topic() or topic,
        partition=message.partition() or 0,
        offset=message.offset() or 0,
        timestamp_ms=timestamp if kind is not TimestampType.NOT_AVAILABLE else None,
        timestamp_type=kind,
        key=message.key(),
        value=value,
        headers=tuple(_to_header(k, v) for k, v in _header_items(message)),
        original_value_size=original_size,
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
