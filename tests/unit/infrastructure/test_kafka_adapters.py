"""Adapter tests using real confluent-kafka metadata classes, without a broker."""

import concurrent.futures
from typing import Any

import pytest
from confluent_kafka import (
    KafkaError,
    KafkaException,
    Node,
    TopicCollection,
    TopicPartitionInfo,
)
from confluent_kafka.admin._metadata import (
    BrokerMetadata,
    ClusterMetadata,
    PartitionMetadata,
    TopicMetadata,
)
from confluent_kafka.admin._topic import TopicDescription

from streamsage.domain.entities.cluster import Broker
from streamsage.domain.exceptions import (
    KafkaConnectionError,
    KafkaMetadataError,
    KafkaOperationTimeoutError,
    KafkaResourceNotFoundError,
)
from streamsage.infrastructure.kafka import gateway as gateway_module
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway
from streamsage.infrastructure.kafka.kafka_cluster_adapter import KafkaClusterAdapter
from streamsage.infrastructure.kafka.kafka_topic_adapter import KafkaTopicAdapter


def _broker(broker_id: int, host: str, port: int) -> BrokerMetadata:
    broker = BrokerMetadata()
    broker.id, broker.host, broker.port = broker_id, host, port
    return broker


def _partition(
    pid: int,
    leader: int,
    replicas: list[int],
    isrs: list[int],
    error: KafkaError | None = None,
) -> PartitionMetadata:
    partition = PartitionMetadata()
    partition.id, partition.leader = pid, leader
    partition.replicas, partition.isrs, partition.error = replicas, isrs, error
    return partition


def _topic(
    name: str, partitions: list[PartitionMetadata], error: KafkaError | None = None
) -> TopicMetadata:
    topic = TopicMetadata()
    topic.topic, topic.error = name, error
    topic.partitions = {p.id: p for p in partitions}
    return topic


def _metadata(
    topics: list[TopicMetadata] | None = None,
    *,
    cluster_id: str | None = "cid",
    controller_id: int = 2,
) -> ClusterMetadata:
    metadata = ClusterMetadata()
    metadata.cluster_id = cluster_id  # type: ignore[assignment]
    metadata.controller_id = controller_id
    metadata.brokers = {2: _broker(2, "b2", 9093), 1: _broker(1, "b1", 9092)}
    metadata.topics = {t.topic: t for t in topics or [] if t.topic}
    return metadata


class FakeAdminClient:
    """Mimics AdminClient: queued error events are delivered on poll()."""

    def __init__(
        self,
        metadata: ClusterMetadata | None = None,
        error: Exception | None = None,
        queued_events: list[KafkaError] | None = None,
        descriptions: dict[str, TopicDescription | Exception | None] | None = None,
    ) -> None:
        self.metadata = metadata or _metadata()
        self.error = error
        self.calls: list[dict[str, Any]] = []
        # Topic name -> description, exception to fail with, or None to hang.
        self.descriptions = descriptions or {}
        self.describe_calls: list[dict[str, Any]] = []
        self.queued_events = list(queued_events or [])
        self.error_events: KafkaErrorEvents | None = None
        self.polls = 0

    def poll(self, timeout: float) -> int:
        assert timeout == 0, "polling must never block"
        self.polls += 1
        delivered = len(self.queued_events)
        if self.error_events is not None:
            for event in self.queued_events:
                self.error_events.record(event)
        self.queued_events.clear()
        return delivered

    def list_topics(self, *args: Any, **kwargs: Any) -> ClusterMetadata:
        self.calls.append({"args": args, "kwargs": kwargs})
        if self.error is not None:
            raise self.error
        return self.metadata

    def describe_topics(
        self, topics: TopicCollection, **kwargs: Any
    ) -> dict[str, concurrent.futures.Future[TopicDescription]]:
        self.describe_calls.append({"topics": topics.topic_names, "kwargs": kwargs})
        futures: dict[str, concurrent.futures.Future[TopicDescription]] = {}
        for name in topics.topic_names:
            future: concurrent.futures.Future[TopicDescription] = (
                concurrent.futures.Future()
            )
            outcome = self.descriptions.get(
                name, KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
            )
            if isinstance(outcome, Exception):
                future.set_exception(outcome)
            elif outcome is not None:
                future.set_result(outcome)
            futures[name] = future
        return futures


def _description(
    name: str, partitions: list[TopicPartitionInfo], *, internal: bool = False
) -> TopicDescription:
    return TopicDescription(name, None, internal, partitions)  # type: ignore[arg-type]


def _partition_info(
    pid: int, leader: int | None, replicas: list[int], isr: list[int]
) -> TopicPartitionInfo:
    node = None if leader is None else Node(leader, "h", 9092)
    return TopicPartitionInfo(
        pid,
        node,  # type: ignore[arg-type]
        [Node(r, "h", 9092) for r in replicas],
        [Node(r, "h", 9092) for r in isr],
    )


def _source(admin: FakeAdminClient, timeout: float = 4) -> KafkaAdminGateway:
    admin.error_events = KafkaErrorEvents()
    return KafkaAdminGateway(
        admin,  # type: ignore[arg-type]
        timeout_seconds=timeout,
        error_events=admin.error_events,
    )


def test_cluster_adapter_maps_metadata() -> None:
    cluster = KafkaClusterAdapter(_source(FakeAdminClient())).get_cluster_info()
    assert cluster.cluster_id == "cid"
    assert cluster.controller_id == 2
    assert cluster.brokers == (Broker(1, "b1", 9092), Broker(2, "b2", 9093))


def test_cluster_adapter_maps_missing_values_to_none() -> None:
    admin = FakeAdminClient(_metadata(cluster_id=None, controller_id=-1))
    cluster = KafkaClusterAdapter(_source(admin)).get_cluster_info()
    assert cluster.cluster_id is None
    assert cluster.controller_id is None


def test_requests_never_scope_metadata_to_a_topic_and_carry_timeouts() -> None:
    admin = FakeAdminClient(
        _metadata([_topic("orders", [_partition(0, 1, [1], [1])])]),
        descriptions={
            "orders": _description("orders", [_partition_info(0, 1, [1], [1])])
        },
    )
    adapter = KafkaTopicAdapter(_source(admin, timeout=7))
    adapter.list_topics()
    adapter.describe_topic("orders")
    with pytest.raises(KafkaResourceNotFoundError):
        adapter.describe_topic("missing")
    # A topic-scoped *Metadata* request could auto-create the topic, so
    # list_topics is only ever called without a topic argument.
    assert admin.calls == [{"args": (), "kwargs": {"timeout": 7}}]
    assert admin.describe_calls == [
        {"topics": ["orders"], "kwargs": {"request_timeout": 7}},
        {"topics": ["missing"], "kwargs": {"request_timeout": 7}},
    ]


def test_topic_adapter_maps_partitions_and_internal_flag() -> None:
    admin = FakeAdminClient(
        _metadata(
            [
                _topic(
                    "orders",
                    [
                        _partition(
                            1,
                            -1,
                            [1, 2],
                            [],
                            KafkaError(KafkaError.LEADER_NOT_AVAILABLE),
                        ),
                        _partition(0, 1, [1, 2], [1, 2]),
                    ],
                ),
                _topic("__consumer_offsets", [_partition(0, 1, [1], [1])]),
            ]
        )
    )
    topics = {t.name: t for t in KafkaTopicAdapter(_source(admin)).list_topics()}

    orders = topics["orders"]
    assert orders.internal is False
    assert [p.partition_id for p in orders.partitions] == [0, 1]
    assert orders.partitions[0].leader == 1
    assert orders.partitions[0].replicas == (1, 2)
    assert orders.partitions[1].leader is None
    assert orders.partitions[1].isr == ()
    assert orders.partitions[1].metadata_error == "LEADER_NOT_AVAILABLE"
    assert topics["__consumer_offsets"].internal is True


def test_describe_topic_maps_description() -> None:
    admin = FakeAdminClient(
        descriptions={
            "__consumer_offsets": _description(
                "__consumer_offsets",
                [
                    _partition_info(1, None, [1, 2], []),
                    _partition_info(0, 2, [1, 2], [2, 1]),
                ],
                internal=True,
            )
        }
    )
    topic = KafkaTopicAdapter(_source(admin)).describe_topic("__consumer_offsets")
    assert topic.internal is True  # flag reported by Kafka, not the name prefix
    assert [p.partition_id for p in topic.partitions] == [0, 1]
    assert topic.partitions[0].leader == 2
    assert topic.partitions[0].replicas == (1, 2)
    assert topic.partitions[0].isr == (2, 1)
    assert topic.partitions[1].leader is None
    assert topic.partitions[1].isr == ()
    assert topic.metadata_error is None


def test_describe_topic_treats_negative_leader_id_as_no_leader() -> None:
    admin = FakeAdminClient(
        descriptions={"t": _description("t", [_partition_info(0, -1, [1], [])])}
    )
    assert (
        KafkaTopicAdapter(_source(admin)).describe_topic("t").partitions[0].leader
        is None
    )


def test_describe_missing_topic_is_not_found() -> None:
    with pytest.raises(
        KafkaResourceNotFoundError, match="Topic 'ghost' does not exist"
    ):
        KafkaTopicAdapter(_source(FakeAdminClient())).describe_topic("ghost")


def test_describe_topic_other_errors_are_translated() -> None:
    admin = FakeAdminClient(
        descriptions={
            "t": KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
        }
    )
    with pytest.raises(KafkaMetadataError, match="TOPIC_AUTHORIZATION_FAILED"):
        KafkaTopicAdapter(_source(admin)).describe_topic("t")


def test_describe_topic_future_that_never_completes_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gateway_module, "_FUTURE_GRACE_SECONDS", 0.05)
    admin = FakeAdminClient(descriptions={"t": None})
    with pytest.raises(KafkaOperationTimeoutError, match="topic description"):
        KafkaTopicAdapter(_source(admin, timeout=0.05)).describe_topic("t")
    assert admin.polls == 1


def test_listing_skips_topics_reported_as_unknown() -> None:
    admin = FakeAdminClient(
        _metadata(
            [
                _topic("ghost", [], KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
                _topic("real", [_partition(0, 1, [1], [1])]),
            ]
        )
    )
    assert [t.name for t in KafkaTopicAdapter(_source(admin)).list_topics()] == ["real"]


def test_kafka_exceptions_are_translated() -> None:
    admin = FakeAdminClient(error=KafkaException(KafkaError(KafkaError._TRANSPORT)))
    with pytest.raises(KafkaConnectionError):
        KafkaTopicAdapter(_source(admin)).list_topics()
    with pytest.raises(KafkaConnectionError):
        KafkaClusterAdapter(_source(admin)).get_cluster_info()


def test_client_events_are_drained_after_success_and_failure() -> None:
    admin = FakeAdminClient(queued_events=[KafkaError(KafkaError._TRANSPORT)])
    KafkaClusterAdapter(_source(admin)).get_cluster_info()
    assert admin.polls == 1

    admin.error = KafkaException(KafkaError(KafkaError._TRANSPORT))
    with pytest.raises(KafkaConnectionError):
        KafkaClusterAdapter(_source(admin)).get_cluster_info()
    assert admin.polls == 2


@pytest.mark.parametrize(
    "event,expected_text",
    [
        (KafkaError._AUTHENTICATION, "Authentication with the Kafka cluster failed"),
        (KafkaError._SSL, "TLS handshake"),
    ],
)
def test_failure_cause_comes_from_client_error_events(
    event: int, expected_text: str
) -> None:
    admin = FakeAdminClient(
        error=KafkaException(KafkaError(KafkaError._TRANSPORT)),
        queued_events=[KafkaError(event), KafkaError(KafkaError._ALL_BROKERS_DOWN)],
    )
    with pytest.raises(KafkaConnectionError, match=expected_text):
        KafkaTopicAdapter(_source(admin)).list_topics()


def test_events_from_before_a_success_do_not_leak_into_later_errors() -> None:
    admin = FakeAdminClient(queued_events=[KafkaError(KafkaError._AUTHENTICATION)])
    source = _source(admin)
    KafkaClusterAdapter(source).get_cluster_info()  # drains the old event

    admin.error = KafkaException(KafkaError(KafkaError._TRANSPORT))
    with pytest.raises(KafkaConnectionError, match="Could not connect"):
        KafkaClusterAdapter(source).get_cluster_info()
