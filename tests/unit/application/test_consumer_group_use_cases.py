import pytest

from streamsage.application.use_cases.describe_consumer_group import (
    DescribeConsumerGroupUseCase,
)
from streamsage.application.use_cases.get_consumer_lag import GetConsumerLagUseCase
from streamsage.application.use_cases.list_consumer_groups import (
    ListConsumerGroupsUseCase,
)
from streamsage.domain.entities.consumer_group import ConsumerGroupState
from streamsage.domain.exceptions import (
    InvalidInputError,
    KafkaConnectionError,
    KafkaResourceNotFoundError,
)
from tests.fakes import SAMPLE_GROUP, FakeConsumerGroupReader


def test_list_groups_sorted_by_id() -> None:
    result = ListConsumerGroupsUseCase(FakeConsumerGroupReader()).execute()
    assert [g.group_id for g in result] == ["billing", "legacy", "orders-service"]


def test_list_groups_filtered_by_state() -> None:
    result = ListConsumerGroupsUseCase(FakeConsumerGroupReader()).execute(
        states=[ConsumerGroupState.STABLE, ConsumerGroupState.EMPTY]
    )
    assert [g.group_id for g in result] == ["billing", "orders-service"]


def test_list_groups_empty_filter_returns_all() -> None:
    assert len(ListConsumerGroupsUseCase(FakeConsumerGroupReader()).execute([])) == 3


def test_describe_group() -> None:
    assert (
        DescribeConsumerGroupUseCase(FakeConsumerGroupReader()).execute("billing")
        == SAMPLE_GROUP
    )


def test_describe_group_validates_id_before_querying() -> None:
    reader = FakeConsumerGroupReader()
    with pytest.raises(InvalidInputError):
        DescribeConsumerGroupUseCase(reader).execute("bad\nid")
    assert reader.requested == []


def test_describe_missing_group() -> None:
    with pytest.raises(KafkaResourceNotFoundError):
        DescribeConsumerGroupUseCase(FakeConsumerGroupReader()).execute("missing")


def test_consumer_lag_is_ordered_and_aggregated() -> None:
    lag = GetConsumerLagUseCase(FakeConsumerGroupReader()).execute("billing")
    assert lag.group_id == "billing"
    assert [p.partition for p in lag.partitions] == [0, 2, 1]
    assert lag.total_lag == 70
    assert lag.unknown_lag_partition_count == 1


def test_consumer_lag_validates_id_and_propagates_errors() -> None:
    with pytest.raises(InvalidInputError):
        GetConsumerLagUseCase(FakeConsumerGroupReader()).execute("")
    reader = FakeConsumerGroupReader(error=KafkaConnectionError("down"))
    with pytest.raises(KafkaConnectionError):
        GetConsumerLagUseCase(reader).execute("billing")
