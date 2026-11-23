"""KafkaMessageAdapter tests with a fake consumer and real Message objects."""

import concurrent.futures
from typing import Any

import pytest
from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
    KafkaError,
    KafkaException,
    Message,
    Node,
    TopicCollection,
    TopicPartition,
    TopicPartitionInfo,
)
from confluent_kafka.admin import ListOffsetsResultInfo, TopicDescription

from streamsage.domain.entities.message import (
    OffsetRange,
    PartitionOffsets,
    RawHeader,
    TimestampType,
)
from streamsage.domain.exceptions import (
    ConfigurationError,
    KafkaConnectionError,
    KafkaResourceNotFoundError,
)
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka import consumer as consumer_module
from streamsage.infrastructure.kafka import kafka_message_adapter as adapter_module
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.consumer import (
    INSPECTOR_GROUP_ID,
    KafkaConsumerFactory,
    build_consumer_config,
)
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway
from streamsage.infrastructure.kafka.kafka_message_adapter import (
    KafkaMessageAdapter,
    to_raw_message,
)


def _msg(partition: int, offset: int, **kwargs: Any) -> Message:
    return Message(
        topic="t",
        partition=partition,
        offset=offset,
        key=b"k",
        value=b"v",
        timestamp=(TIMESTAMP_CREATE_TIME, 1_000),
        **kwargs,
    )


def _eof(partition: int) -> Message:
    return Message(
        topic="t", partition=partition, error=KafkaError(KafkaError._PARTITION_EOF)
    )


class FakeConsumer:
    def __init__(
        self,
        messages: list[Message | None] | None = None,
        watermarks: dict[int, tuple[int, int]] | None = None,
        watermark_error: KafkaException | None = None,
    ) -> None:
        self.queue = list(messages or [])
        self.watermarks = watermarks or {}
        self.watermark_error = watermark_error
        self.assigned: list[tuple[int, int]] = []
        self.closed = False

    def get_watermark_offsets(
        self, tp: TopicPartition, timeout: float = -1, cached: bool = False
    ) -> tuple[int, int]:
        if self.watermark_error is not None:
            raise self.watermark_error
        return self.watermarks[tp.partition]

    def assign(self, partitions: list[TopicPartition]) -> None:
        self.assigned = [(tp.partition, tp.offset) for tp in partitions]

    def poll(self, timeout: float = -1) -> Message | None:
        return self.queue.pop(0) if self.queue else None

    def close(self) -> None:
        self.closed = True


class FakeFactory:
    def __init__(self, consumer: FakeConsumer) -> None:
        self.consumer = consumer
        self.created = 0

    def create(self, error_events: KafkaErrorEvents) -> FakeConsumer:
        self.created += 1
        return self.consumer


class FakeAdmin:
    """Admin client double: DescribeTopics and batched ListOffsets."""

    def __init__(
        self,
        description: TopicDescription | Exception,
        offsets: dict[int, tuple[int, int] | Exception] | None = None,
    ) -> None:
        self.description = description
        # partition -> (earliest, latest), or an exception for that partition
        self.offsets = offsets if offsets is not None else {0: (0, 10), 1: (5, 7)}
        self.list_offsets_calls: list[tuple[str, int]] = []

    def list_offsets(
        self, requested: dict[TopicPartition, Any], **kwargs: Any
    ) -> dict[TopicPartition, concurrent.futures.Future[Any]]:
        futures: dict[TopicPartition, concurrent.futures.Future[Any]] = {}
        spec_name = type(next(iter(requested.values()))).__name__
        self.list_offsets_calls.append((spec_name, len(requested)))
        for tp in requested:
            future: concurrent.futures.Future[Any] = concurrent.futures.Future()
            outcome = self.offsets[tp.partition]
            if isinstance(outcome, Exception):
                future.set_exception(outcome)
            else:
                low, high = outcome
                offset = low if spec_name == "EarliestSpec" else high
                future.set_result(ListOffsetsResultInfo(offset, -1, 0))
            futures[tp] = future
        return futures

    def poll(self, timeout: float) -> int:
        return 0

    def describe_topics(
        self, topics: TopicCollection, **kwargs: Any
    ) -> dict[str, concurrent.futures.Future[Any]]:
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        if isinstance(self.description, Exception):
            future.set_exception(self.description)
        else:
            future.set_result(self.description)
        return {topics.topic_names[0]: future}


def _description(*partitions: int) -> TopicDescription:
    node = Node(1, "h", 9092)
    return TopicDescription(
        "t",
        None,  # type: ignore[arg-type]
        False,
        [TopicPartitionInfo(p, node, [node], [node]) for p in partitions],
    )


def _adapter(
    consumer: FakeConsumer,
    description: TopicDescription | Exception | None = None,
    offsets: dict[int, tuple[int, int] | Exception] | None = None,
    max_value_bytes: int = 1024,
) -> tuple[KafkaMessageAdapter, FakeFactory]:
    gateway = KafkaAdminGateway(
        FakeAdmin(description or _description(0, 1), offsets),  # type: ignore[arg-type]
        timeout_seconds=1,
        error_events=KafkaErrorEvents(),
    )
    factory = FakeFactory(consumer)
    return (
        KafkaMessageAdapter(
            gateway,  # type: ignore[arg-type]
            factory,  # type: ignore[arg-type]
            timeout_seconds=1,
            max_value_bytes=max_value_bytes,
        ),
        factory,
    )


def test_partition_offsets_use_two_batched_requests_and_no_consumer() -> None:
    consumer = FakeConsumer()
    adapter, factory = _adapter(
        consumer, _description(*range(500)), {p: (p, p + 10) for p in range(500)}
    )
    offsets = adapter.partition_offsets("t")
    assert len(offsets.partitions) == 500
    assert offsets.partitions[7] == PartitionOffsets(7, 7, 17)
    assert offsets.unavailable_partitions == ()
    admin = adapter._gateway._admin_client  # type: ignore[attr-defined]
    assert admin.list_offsets_calls == [("EarliestSpec", 500), ("LatestSpec", 500)]
    assert factory.created == 0  # offsets no longer need a consumer


def test_partition_with_unreadable_offsets_is_reported_unavailable() -> None:
    adapter, _ = _adapter(
        FakeConsumer(),
        offsets={
            0: (0, 10),
            1: KafkaException(KafkaError(KafkaError.LEADER_NOT_AVAILABLE)),
        },
    )
    offsets = adapter.partition_offsets("t")
    assert offsets.partitions == (PartitionOffsets(0, 0, 10),)
    assert offsets.unavailable_partitions == (1,)
    assert offsets.partition_count == 2


def test_partition_offsets_of_missing_topic_is_not_found_without_a_consumer() -> None:
    adapter, factory = _adapter(
        FakeConsumer(),
        KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
    )
    with pytest.raises(KafkaResourceNotFoundError, match="Topic 't'"):
        adapter.partition_offsets("t")
    assert factory.created == 0


def test_connection_failure_during_offset_lookup_is_translated() -> None:
    adapter, _ = _adapter(
        FakeConsumer(),
        offsets={0: KafkaException(KafkaError(KafkaError._TRANSPORT)), 1: (0, 1)},
    )
    with pytest.raises(KafkaConnectionError):
        adapter.partition_offsets("t")


def test_read_newest_keeps_only_the_newest_across_partitions() -> None:
    def stamped(partition: int, offset: int) -> Message:
        return Message(
            topic="t",
            partition=partition,
            offset=offset,
            key=b"k",
            value=b"v",
            timestamp=(TIMESTAMP_CREATE_TIME, offset * 10 + partition),
        )

    queue: list[Message | None] = []
    for offset in range(50):
        queue += [stamped(0, offset), stamped(1, offset)]
    consumer = FakeConsumer(queue)
    adapter, _ = _adapter(consumer)
    result = adapter.read_newest("t", [OffsetRange(0, 0, 50), OffsetRange(1, 0, 50)], 3)
    assert result.messages_read == 100
    assert [(m.partition, m.offset) for m in result.messages] == [
        (1, 49),
        (0, 49),
        (1, 48),
    ]
    assert result.complete is True
    assert consumer.closed is True


def test_read_newest_with_nothing_to_read() -> None:
    adapter, factory = _adapter(FakeConsumer())
    result = adapter.read_newest("t", [OffsetRange(0, 5, 5)], 3)
    assert (result.messages, result.complete) == ((), True)
    assert factory.created == 0


def test_large_payloads_are_dropped_while_reading_but_keep_their_size() -> None:
    big = Message(
        topic="t",
        partition=0,
        offset=0,
        key=b"k",
        value=b"x" * 5_000,
        timestamp=(TIMESTAMP_CREATE_TIME, 1),
    )
    small = Message(
        topic="t",
        partition=0,
        offset=1,
        key=b"k",
        value=b"x" * 10,
        timestamp=(TIMESTAMP_CREATE_TIME, 2),
    )
    adapter, _ = _adapter(FakeConsumer([big, small]), max_value_bytes=1_024)
    result = adapter.read_range("t", [OffsetRange(0, 0, 2)])
    dropped, kept = sorted(result.messages, key=lambda m: m.offset)
    assert (dropped.value, dropped.value_size, dropped.value_truncated) == (
        b"",
        5_000,
        True,
    )
    assert (kept.value, kept.value_truncated) == (b"x" * 10, False)


def test_read_range_reads_only_requested_offsets() -> None:
    consumer = FakeConsumer(
        [
            _msg(0, 3),
            None,
            _msg(1, 0),
            _msg(0, 4),
            _msg(0, 5),  # beyond stop=5: ignored, partition done
            _msg(1, 1),  # last of partition 1 (stop=2)
        ]
    )
    adapter, _ = _adapter(consumer)
    result = adapter.read_range("t", [OffsetRange(0, 3, 5), OffsetRange(1, 0, 2)])
    assert result.complete is True
    assert sorted((m.partition, m.offset) for m in result.messages) == [
        (0, 3),
        (0, 4),
        (1, 0),
        (1, 1),
    ]
    assert consumer.assigned == [(0, 3), (1, 0)]
    assert consumer.closed is True


def test_end_of_partition_completes_a_range() -> None:
    consumer = FakeConsumer([_msg(0, 3), _eof(0)])
    adapter, _ = _adapter(consumer)
    result = adapter.read_range("t", [OffsetRange(0, 3, 10)])
    assert result.complete is True
    assert [m.offset for m in result.messages] == [3]


def test_timeout_returns_partial_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(adapter_module, "_POLL_INTERVAL_SECONDS", 0.01)
    consumer = FakeConsumer([_msg(0, 3)])
    gateway = KafkaAdminGateway(
        FakeAdmin(_description(0)),  # type: ignore[arg-type]
        timeout_seconds=0.05,
        error_events=KafkaErrorEvents(),
    )
    adapter = KafkaMessageAdapter(gateway, FakeFactory(consumer), timeout_seconds=0.05)  # type: ignore[arg-type]
    result = adapter.read_range("t", [OffsetRange(0, 3, 10)])
    assert result.complete is False
    assert [m.offset for m in result.messages] == [3]


def test_empty_ranges_do_not_create_a_consumer() -> None:
    adapter, factory = _adapter(FakeConsumer())
    result = adapter.read_range("t", [OffsetRange(0, 5, 5)])
    assert (result.messages, result.complete) == ((), True)
    assert factory.created == 0


def test_fatal_errors_are_translated() -> None:
    fatal = KafkaError(KafkaError._TRANSPORT, fatal=True)
    consumer = FakeConsumer([Message(topic="t", partition=0, error=fatal)])
    adapter, _ = _adapter(consumer)
    with pytest.raises(KafkaConnectionError):
        adapter.read_range("t", [OffsetRange(0, 0, 5)])
    assert consumer.closed is True


def test_non_fatal_errors_and_other_partitions_are_skipped() -> None:
    consumer = FakeConsumer(
        [
            Message(topic="t", partition=0, error=KafkaError(KafkaError._TRANSPORT)),
            _msg(9, 0),  # not requested
            _msg(0, 0),
        ]
    )
    adapter, _ = _adapter(consumer)
    result = adapter.read_range("t", [OffsetRange(0, 0, 1)])
    assert [m.offset for m in result.messages] == [0]


@pytest.mark.parametrize(
    ("timestamp", "expected_type", "expected_ms"),
    [
        ((TIMESTAMP_CREATE_TIME, 5), TimestampType.CREATE_TIME, 5),
        ((TIMESTAMP_LOG_APPEND_TIME, 6), TimestampType.LOG_APPEND_TIME, 6),
        ((TIMESTAMP_NOT_AVAILABLE, 0), TimestampType.NOT_AVAILABLE, None),
    ],
)
def test_message_conversion(
    timestamp: tuple[int, int], expected_type: TimestampType, expected_ms: int | None
) -> None:
    message = Message(
        topic="t",
        partition=2,
        offset=9,
        key=None,
        value=b"v",
        headers=[("a", b"1"), ("b", None), ("c", "text")],
        timestamp=timestamp,
    )
    raw = to_raw_message(message, "t")
    assert (raw.partition, raw.offset, raw.key, raw.value) == (2, 9, None, b"v")
    assert (raw.timestamp_type, raw.timestamp_ms) == (expected_type, expected_ms)
    assert raw.headers == (
        RawHeader("a", b"1"),
        RawHeader("b", None),
        RawHeader("c", b"text"),
    )


def test_consumer_config_never_commits_or_subscribes() -> None:
    config = build_consumer_config(Settings())
    assert config["group.id"] == INSPECTOR_GROUP_ID
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["enable.partition.eof"] is True


def test_consumer_factory_passes_logger_and_error_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        consumer_module, "Consumer", lambda config: captured.update(config)
    )
    events = KafkaErrorEvents()
    KafkaConsumerFactory(Settings()).create(events)
    assert captured["error_cb"] == events.record
    assert captured["logger"].name == "streamsage.kafka.librdkafka"


@pytest.mark.parametrize(
    "error", [ValueError("bad"), KafkaException(KafkaError(KafkaError._INVALID_ARG))]
)
def test_consumer_factory_rejected_config(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    def fail(config: dict[str, Any]) -> None:
        raise error

    monkeypatch.setattr(consumer_module, "Consumer", fail)
    with pytest.raises(ConfigurationError, match="rejected the configuration"):
        KafkaConsumerFactory(Settings()).create(KafkaErrorEvents())


def test_find_by_key_keeps_only_newest_matches_per_partition() -> None:
    def keyed(partition: int, offset: int, key: bytes) -> Message:
        return Message(
            topic="t",
            partition=partition,
            offset=offset,
            key=key,
            value=b"v",
            timestamp=(TIMESTAMP_CREATE_TIME, offset),
        )

    consumer = FakeConsumer(
        [
            keyed(0, 0, b"a"),
            keyed(0, 1, b"b"),
            keyed(0, 2, b"a"),
            keyed(1, 0, b"a"),
            keyed(0, 3, b"a"),
            keyed(1, 1, b"c"),
        ]
    )
    adapter, _ = _adapter(consumer)
    result = adapter.find_by_key(
        "t", [OffsetRange(0, 0, 4), OffsetRange(1, 0, 2)], b"a", 2
    )
    assert result.scanned == 6
    assert result.complete is True
    assert sorted((m.partition, m.offset) for m in result.matches) == [
        (0, 2),
        (0, 3),
        (1, 0),
    ]
    assert consumer.closed is True


def test_find_by_key_with_nothing_to_scan() -> None:
    adapter, factory = _adapter(FakeConsumer())
    result = adapter.find_by_key("t", [OffsetRange(0, 4, 4)], b"a", 5)
    assert (result.matches, result.scanned, result.complete) == ((), 0, True)
    assert factory.created == 0


def test_find_by_key_failures_are_translated() -> None:
    fatal = KafkaError(KafkaError._TRANSPORT, fatal=True)
    consumer = FakeConsumer([Message(topic="t", partition=0, error=fatal)])
    adapter, _ = _adapter(consumer)
    with pytest.raises(KafkaConnectionError):
        adapter.find_by_key("t", [OffsetRange(0, 0, 5)], b"a", 5)
    assert consumer.closed is True


def test_read_newest_failures_are_translated_and_consumer_closed() -> None:
    fatal = KafkaError(KafkaError._TRANSPORT, fatal=True)
    consumer = FakeConsumer([Message(topic="t", partition=0, error=fatal)])
    adapter, _ = _adapter(consumer)
    with pytest.raises(KafkaConnectionError):
        adapter.read_newest("t", [OffsetRange(0, 0, 5)], 3)
    assert consumer.closed is True
