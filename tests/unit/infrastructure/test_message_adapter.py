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
from confluent_kafka.admin._topic import TopicDescription

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
    def __init__(self, description: TopicDescription | Exception) -> None:
        self.description = description

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
    consumer: FakeConsumer, description: TopicDescription | Exception | None = None
) -> tuple[KafkaMessageAdapter, FakeFactory]:
    gateway = KafkaAdminGateway(
        FakeAdmin(description or _description(0, 1)),  # type: ignore[arg-type]
        timeout_seconds=1,
        error_events=KafkaErrorEvents(),
    )
    factory = FakeFactory(consumer)
    return (
        KafkaMessageAdapter(gateway, factory, timeout_seconds=1),  # type: ignore[arg-type]
        factory,
    )


def test_partition_offsets_uses_describe_then_watermarks() -> None:
    consumer = FakeConsumer(watermarks={0: (0, 10), 1: (5, 7)})
    adapter, _ = _adapter(consumer)
    assert adapter.partition_offsets("t") == [
        PartitionOffsets(0, 0, 10),
        PartitionOffsets(1, 5, 7),
    ]
    assert consumer.closed is True


def test_partition_offsets_of_missing_topic_is_not_found_without_a_consumer() -> None:
    adapter, factory = _adapter(
        FakeConsumer(),
        KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
    )
    with pytest.raises(KafkaResourceNotFoundError, match="Topic 't'"):
        adapter.partition_offsets("t")
    assert factory.created == 0


def test_watermark_failure_is_translated_and_consumer_closed() -> None:
    consumer = FakeConsumer(
        watermark_error=KafkaException(KafkaError(KafkaError._TRANSPORT))
    )
    adapter, _ = _adapter(consumer)
    with pytest.raises(KafkaConnectionError):
        adapter.partition_offsets("t")
    assert consumer.closed is True


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
