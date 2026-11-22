import pytest

from streamsage.application.use_cases.describe_topic import DescribeTopicUseCase
from streamsage.application.use_cases.get_cluster_info import GetClusterInfoUseCase
from streamsage.application.use_cases.list_topics import ListTopicsUseCase
from streamsage.domain.entities.topic import Partition, Topic
from streamsage.domain.exceptions import (
    InvalidInputError,
    KafkaConnectionError,
    KafkaResourceNotFoundError,
)
from tests.fakes import SAMPLE_CLUSTER, FakeClusterReader, FakeTopicReader, make_topic


def test_get_cluster_info_returns_reader_result() -> None:
    assert GetClusterInfoUseCase(FakeClusterReader()).execute() == SAMPLE_CLUSTER


def test_get_cluster_info_propagates_errors() -> None:
    reader = FakeClusterReader(error=KafkaConnectionError("down"))
    with pytest.raises(KafkaConnectionError):
        GetClusterInfoUseCase(reader).execute()


def _topics() -> list[Topic]:
    return [
        make_topic("payments"),
        make_topic("__consumer_offsets", 50),
        make_topic("orders"),
    ]


def test_list_topics_hides_internal_and_sorts_by_default() -> None:
    result = ListTopicsUseCase(FakeTopicReader(_topics())).execute()
    assert [t.name for t in result] == ["orders", "payments"]


def test_list_topics_can_include_internal() -> None:
    result = ListTopicsUseCase(FakeTopicReader(_topics())).execute(
        include_internal=True
    )
    assert [t.name for t in result] == ["__consumer_offsets", "orders", "payments"]


def test_list_topics_empty_cluster() -> None:
    assert ListTopicsUseCase(FakeTopicReader([])).execute() == []


def test_describe_topic_orders_partitions() -> None:
    unordered = Topic(
        "orders",
        (
            Partition(2, 1, (1,), (1,)),
            Partition(0, 1, (1,), (1,)),
            Partition(1, 1, (1,), (1,)),
        ),
    )
    result = DescribeTopicUseCase(FakeTopicReader([unordered])).execute("orders")
    assert [p.partition_id for p in result.partitions] == [0, 1, 2]


def test_describe_topic_validates_name_before_querying() -> None:
    reader = FakeTopicReader([make_topic("orders")])
    with pytest.raises(InvalidInputError):
        DescribeTopicUseCase(reader).execute("not a topic")
    assert reader.described == []


def test_describe_topic_not_found() -> None:
    with pytest.raises(KafkaResourceNotFoundError):
        DescribeTopicUseCase(FakeTopicReader([])).execute("missing")
