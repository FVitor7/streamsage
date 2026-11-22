import pytest

from streamsage.domain.entities.consumer_group import TopicPartitionRef
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.value_objects.group_id import (
    MAX_GROUP_ID_LENGTH,
    validate_group_id,
)
from streamsage.domain.value_objects.lag import ConsumerGroupLag, PartitionLag
from tests.fakes import SAMPLE_GROUP


@pytest.mark.parametrize(
    ("committed", "end", "expected"),
    [
        (40, 100, 60),
        (100, 100, 0),
        (0, 0, 0),
        (None, 100, None),  # never committed: unknown, not 0
        (40, None, None),  # end offset unreadable: unknown
        (None, None, None),
        (105, 100, 0),  # offsets read at slightly different moments
    ],
)
def test_partition_lag(
    committed: int | None, end: int | None, expected: int | None
) -> None:
    assert PartitionLag("t", 0, committed, end).lag == expected


def test_group_lag_orders_partitions_and_aggregates_known_values() -> None:
    lag = ConsumerGroupLag(
        "g",
        (
            PartitionLag("b", 0, 10, 20),  # 10
            PartitionLag("a", 1, None, 50),  # unknown
            PartitionLag("a", 0, 0, 30),  # 30
            PartitionLag("b", 1, 5, 15),  # 10
        ),
    )
    assert [(p.topic, p.partition) for p in lag.partitions] == [
        ("a", 0),
        ("b", 0),
        ("b", 1),
        ("a", 1),
    ]
    assert lag.total_lag == 50
    assert lag.max_lag == 30
    assert lag.unknown_lag_partition_count == 1


def test_group_lag_with_only_unknown_values_is_unknown() -> None:
    lag = ConsumerGroupLag("g", (PartitionLag("a", 0, None, 10),))
    assert lag.total_lag is None
    assert lag.max_lag is None
    assert lag.unknown_lag_partition_count == 1


def test_group_lag_without_partitions() -> None:
    lag = ConsumerGroupLag("g", ())
    assert lag.total_lag is None
    assert lag.unknown_lag_partition_count == 0


def test_consumer_group_derived_properties() -> None:
    assert SAMPLE_GROUP.member_count == 1
    assert SAMPLE_GROUP.assigned_partitions == (
        TopicPartitionRef("payments", 0),
        TopicPartitionRef("payments", 1),
    )


@pytest.mark.parametrize(
    "group_id", ["orders-service", "a", "Group With Spaces", "x" * MAX_GROUP_ID_LENGTH]
)
def test_valid_group_ids(group_id: str) -> None:
    assert validate_group_id(group_id) == group_id


@pytest.mark.parametrize(
    "group_id", ["", "   ", "x" * (MAX_GROUP_ID_LENGTH + 1), "bad\nid", "tab\tid"]
)
def test_invalid_group_ids(group_id: str) -> None:
    with pytest.raises(InvalidInputError):
        validate_group_id(group_id)
