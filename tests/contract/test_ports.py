"""Kafka adapters must keep satisfying the application ports."""

import inspect

import pytest

from streamsage.application.ports.cluster_reader import ClusterReader
from streamsage.application.ports.topic_reader import TopicReader
from streamsage.infrastructure.kafka.kafka_cluster_adapter import KafkaClusterAdapter
from streamsage.infrastructure.kafka.kafka_topic_adapter import KafkaTopicAdapter
from tests.fakes import FakeClusterReader, FakeTopicReader

PAIRS = [
    (ClusterReader, KafkaClusterAdapter, ["get_cluster_info"]),
    (TopicReader, KafkaTopicAdapter, ["list_topics", "describe_topic"]),
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


@pytest.mark.parametrize("adapter", [KafkaClusterAdapter, KafkaTopicAdapter])
def test_adapters_expose_no_write_operations(adapter: type) -> None:
    forbidden = ("create", "delete", "alter", "produce", "commit", "reset", "update")
    public = [name for name in dir(adapter) if not name.startswith("_")]
    assert not [n for n in public if n.startswith(forbidden)]
