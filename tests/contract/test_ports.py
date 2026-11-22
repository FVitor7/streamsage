"""Kafka adapters must keep satisfying the application ports."""

import inspect

import pytest

from streamsage.application.ports.cluster_reader import ClusterReader
from streamsage.application.ports.consumer_group_reader import ConsumerGroupReader
from streamsage.application.ports.message_reader import MessageReader
from streamsage.application.ports.topic_reader import TopicReader
from streamsage.infrastructure.kafka.kafka_cluster_adapter import KafkaClusterAdapter
from streamsage.infrastructure.kafka.kafka_consumer_group_adapter import (
    KafkaConsumerGroupAdapter,
)
from streamsage.infrastructure.kafka.kafka_message_adapter import KafkaMessageAdapter
from streamsage.infrastructure.kafka.kafka_topic_adapter import KafkaTopicAdapter
from tests.fakes import (
    FakeClusterReader,
    FakeConsumerGroupReader,
    FakeMessageReader,
    FakeTopicReader,
)

PAIRS = [
    (ClusterReader, KafkaClusterAdapter, ["get_cluster_info"]),
    (TopicReader, KafkaTopicAdapter, ["list_topics", "describe_topic"]),
    (
        ConsumerGroupReader,
        KafkaConsumerGroupAdapter,
        ["list_consumer_groups", "describe_consumer_group", "get_consumer_lag"],
    ),
    (
        MessageReader,
        KafkaMessageAdapter,
        ["partition_offsets", "read_range", "find_by_key"],
    ),
]


@pytest.mark.parametrize(("port", "adapter", "methods"), PAIRS)
def test_adapter_signatures_match_port(
    port: type, adapter: type, methods: list[str]
) -> None:
    for method in methods:
        expected = inspect.signature(getattr(port, method))
        actual = inspect.signature(getattr(adapter, method))
        assert actual == expected, f"{adapter.__name__}.{method}"


def test_adapters_and_fakes_satisfy_ports_structurally() -> None:
    cluster = KafkaClusterAdapter(gateway=None)  # type: ignore[arg-type]
    topic = KafkaTopicAdapter(gateway=None)  # type: ignore[arg-type]
    assert isinstance(cluster, ClusterReader)
    assert isinstance(topic, TopicReader)
    assert isinstance(FakeClusterReader(), ClusterReader)
    assert isinstance(FakeTopicReader(), TopicReader)
    groups = KafkaConsumerGroupAdapter(gateway=None)  # type: ignore[arg-type]
    assert isinstance(groups, ConsumerGroupReader)
    assert isinstance(FakeConsumerGroupReader(), ConsumerGroupReader)
    messages = KafkaMessageAdapter(None, None, 1)  # type: ignore[arg-type]
    assert isinstance(messages, MessageReader)
    assert isinstance(FakeMessageReader(), MessageReader)


@pytest.mark.parametrize(
    "adapter",
    [
        KafkaClusterAdapter,
        KafkaTopicAdapter,
        KafkaConsumerGroupAdapter,
        KafkaMessageAdapter,
    ],
)
def test_adapters_expose_no_write_operations(adapter: type) -> None:
    forbidden = ("create", "delete", "alter", "produce", "commit", "reset", "update")
    public = [name for name in dir(adapter) if not name.startswith("_")]
    assert not [n for n in public if n.startswith(forbidden)]
