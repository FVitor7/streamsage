import time
import uuid

import pytest

from streamsage.application.use_cases.describe_topic import DescribeTopicUseCase
from streamsage.application.use_cases.get_cluster_info import GetClusterInfoUseCase
from streamsage.application.use_cases.list_topics import ListTopicsUseCase
from streamsage.domain.exceptions import (
    KafkaConnectionError,
    KafkaResourceNotFoundError,
)
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.admin_client import KafkaAdminClientFactory
from streamsage.infrastructure.kafka.kafka_cluster_adapter import KafkaClusterAdapter
from streamsage.infrastructure.kafka.kafka_topic_adapter import KafkaTopicAdapter
from streamsage.infrastructure.kafka.metadata import KafkaMetadataSource
from tests.integration.conftest import SAMPLE_TOPICS

pytestmark = pytest.mark.integration


def test_get_cluster_info(metadata_source: KafkaMetadataSource) -> None:
    cluster = GetClusterInfoUseCase(KafkaClusterAdapter(metadata_source)).execute()
    assert cluster.cluster_id
    assert cluster.broker_count >= 1
    assert all(broker.port > 0 and broker.host for broker in cluster.brokers)
    assert cluster.controller_id in {b.broker_id for b in cluster.brokers}


def test_list_topics_returns_sample_topics(
    metadata_source: KafkaMetadataSource,
) -> None:
    topics = ListTopicsUseCase(KafkaTopicAdapter(metadata_source)).execute()
    by_name = {topic.name: topic for topic in topics}
    for name, partitions in SAMPLE_TOPICS.items():
        assert by_name[name].partition_count == partitions
    assert [t.name for t in topics] == sorted(by_name)


def test_internal_topics_are_filtered_by_default(
    metadata_source: KafkaMetadataSource,
) -> None:
    use_case = ListTopicsUseCase(KafkaTopicAdapter(metadata_source))
    default_names = {t.name for t in use_case.execute()}
    all_topics = use_case.execute(include_internal=True)

    assert "__consumer_offsets" in {t.name for t in all_topics}
    assert "__consumer_offsets" not in default_names
    assert all(t.internal for t in all_topics if t.name.startswith("__"))


def test_describe_topic(metadata_source: KafkaMetadataSource) -> None:
    topic = DescribeTopicUseCase(KafkaTopicAdapter(metadata_source)).execute(
        "orders.created"
    )
    assert [p.partition_id for p in topic.partitions] == [0, 1, 2]
    for partition in topic.partitions:
        assert partition.leader is not None
        assert partition.replicas
        assert set(partition.isr) <= set(partition.replicas)
    assert topic.metadata_error is None


def test_missing_topic_raises_and_is_never_created(
    metadata_source: KafkaMetadataSource,
) -> None:
    # The local broker has auto.create.topics.enable=true on purpose.
    name = f"streamsage.it.missing.{uuid.uuid4().hex}"
    adapter = KafkaTopicAdapter(metadata_source)

    with pytest.raises(KafkaResourceNotFoundError):
        DescribeTopicUseCase(adapter).execute(name)

    # Broker-side auto-creation is asynchronous; give it time to show up.
    time.sleep(3)
    assert name not in {t.name for t in adapter.list_topics()}
    with pytest.raises(KafkaResourceNotFoundError):
        adapter.describe_topic(name)


def test_unreachable_broker_fails_fast_with_controlled_error() -> None:
    settings = Settings(
        kafka_bootstrap_servers="127.0.0.1:1",
        kafka_request_timeout=2,
        kafka_socket_timeout=2,
    )
    with KafkaAdminClientFactory(settings).create() as client:
        adapter = KafkaClusterAdapter(KafkaMetadataSource(client, timeout_seconds=2))
        started = time.monotonic()
        with pytest.raises(KafkaConnectionError, match="waited up to 2s"):
            adapter.get_cluster_info()
        assert time.monotonic() - started < 6
