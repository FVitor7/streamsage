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
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway
from streamsage.infrastructure.kafka.kafka_cluster_adapter import KafkaClusterAdapter
from streamsage.infrastructure.kafka.kafka_topic_adapter import KafkaTopicAdapter
from tests.integration.conftest import SAMPLE_TOPICS

pytestmark = pytest.mark.integration


def test_get_cluster_info(gateway: KafkaAdminGateway) -> None:
    cluster = GetClusterInfoUseCase(KafkaClusterAdapter(gateway)).execute()
    assert cluster.cluster_id
    assert cluster.broker_count >= 1
    assert all(broker.port > 0 and broker.host for broker in cluster.brokers)
    assert cluster.controller_id in {b.broker_id for b in cluster.brokers}


def test_list_topics_returns_sample_topics(
    gateway: KafkaAdminGateway,
) -> None:
    topics = ListTopicsUseCase(KafkaTopicAdapter(gateway)).execute()
    by_name = {topic.name: topic for topic in topics}
    for name, partitions in SAMPLE_TOPICS.items():
        assert by_name[name].partition_count == partitions
    assert [t.name for t in topics] == sorted(by_name)


def test_internal_topics_are_filtered_by_default(
    gateway: KafkaAdminGateway,
) -> None:
    use_case = ListTopicsUseCase(KafkaTopicAdapter(gateway))
    default_names = {t.name for t in use_case.execute()}
    all_topics = use_case.execute(include_internal=True)

    assert "__consumer_offsets" in {t.name for t in all_topics}
    assert "__consumer_offsets" not in default_names
    assert all(t.internal for t in all_topics if t.name.startswith("__"))


def test_describe_topic(gateway: KafkaAdminGateway) -> None:
    topic = DescribeTopicUseCase(KafkaTopicAdapter(gateway)).execute("orders.created")
    assert [p.partition_id for p in topic.partitions] == [0, 1, 2]
    for partition in topic.partitions:
        assert partition.leader is not None
        assert partition.replicas
        assert set(partition.isr) <= set(partition.replicas)
    assert topic.metadata_error is None


def test_missing_topic_raises_and_is_never_created(
    gateway: KafkaAdminGateway,
) -> None:
    # The local broker has auto.create.topics.enable=true on purpose.
    name = f"streamsage.it.missing.{uuid.uuid4().hex}"
    adapter = KafkaTopicAdapter(gateway)

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
    events = KafkaErrorEvents()
    with KafkaAdminClientFactory(settings).create(events) as client:
        adapter = KafkaClusterAdapter(
            KafkaAdminGateway(client, timeout_seconds=2, error_events=events)
        )
        started = time.monotonic()
        with pytest.raises(KafkaConnectionError, match="waited up to 2s"):
            adapter.get_cluster_info()
        assert time.monotonic() - started < 6


def test_describe_internal_topic_uses_kafka_flag(gateway: KafkaAdminGateway) -> None:
    topic = DescribeTopicUseCase(KafkaTopicAdapter(gateway)).execute(
        "__consumer_offsets"
    )
    assert topic.internal is True
    assert topic.partition_count > 0
