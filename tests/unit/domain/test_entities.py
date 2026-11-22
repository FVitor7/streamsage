from dataclasses import FrozenInstanceError

import pytest

from streamsage.domain.entities.cluster import Broker, Cluster
from streamsage.domain.entities.topic import Partition, Topic


def test_cluster_is_immutable_and_counts_brokers() -> None:
    cluster = Cluster("id", (Broker(1, "h", 9092), Broker(2, "h2", 9092)), 1)
    assert cluster.broker_count == 2
    with pytest.raises(FrozenInstanceError):
        cluster.cluster_id = "other"  # type: ignore[misc]


def test_cluster_allows_unknown_id_and_controller() -> None:
    cluster = Cluster(cluster_id=None, brokers=(), controller_id=None)
    assert cluster.cluster_id is None
    assert cluster.controller_id is None
    assert cluster.broker_count == 0


@pytest.mark.parametrize(
    ("replicas", "isr", "expected"),
    [
        ((1, 2, 3), (1, 2, 3), False),
        ((1, 2, 3), (1, 2), True),
        ((1, 2, 3), (), True),
        ((), (), False),
    ],
)
def test_partition_under_replication(
    replicas: tuple[int, ...], isr: tuple[int, ...], expected: bool
) -> None:
    partition = Partition(0, leader=1, replicas=replicas, isr=isr)
    assert partition.is_under_replicated is expected


def test_partition_without_leader() -> None:
    assert Partition(0, leader=None, replicas=(1,), isr=()).has_leader is False
    assert Partition(0, leader=1, replicas=(1,), isr=(1,)).has_leader is True


def test_topic_derived_properties() -> None:
    healthy = Partition(0, leader=1, replicas=(1, 2), isr=(1, 2))
    lagging = Partition(1, leader=2, replicas=(1, 2, 3), isr=(2,))
    offline = Partition(2, leader=None, replicas=(3,), isr=())
    topic = Topic("orders", (healthy, lagging, offline))

    assert topic.partition_count == 3
    assert topic.replication_factor == 3
    assert topic.under_replicated_partitions == (lagging, offline)
    assert topic.leaderless_partitions == (offline,)
    assert topic.internal is False
    assert topic.metadata_error is None


def test_topic_without_partitions_has_unknown_replication_factor() -> None:
    topic = Topic("empty", ())
    assert topic.partition_count == 0
    assert topic.replication_factor is None
