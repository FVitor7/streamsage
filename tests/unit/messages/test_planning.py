import pytest

from streamsage.domain.entities.message import OffsetRange, PartitionOffsets
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.messages.planning import plan_from_offset, plan_latest

OFFSETS = [
    PartitionOffsets(1, low=0, high=3),
    PartitionOffsets(0, low=10, high=100),
    PartitionOffsets(2, low=5, high=5),  # empty
]


def test_latest_reads_newest_messages_of_each_partition() -> None:
    plan = plan_latest(OFFSETS, limit=5)
    assert plan.ranges == (OffsetRange(0, 95, 100), OffsetRange(1, 0, 3))
    assert plan.notes == ("Partition(s) 2 currently hold no messages.",)


def test_latest_on_one_partition() -> None:
    plan = plan_latest(OFFSETS, limit=200, partition=0)
    assert plan.ranges == (OffsetRange(0, 10, 100),)  # bounded by retention
    assert plan.notes == ()


def test_unknown_partition_is_rejected() -> None:
    with pytest.raises(InvalidInputError, match=r"available: 0, 1, 2"):
        plan_latest(OFFSETS, limit=5, partition=9)


def test_from_offset() -> None:
    assert plan_from_offset(OFFSETS, 0, start=50, limit=10).ranges == (
        OffsetRange(0, 50, 60),
    )
    assert plan_from_offset(OFFSETS, 0, start=95, limit=10).ranges == (
        OffsetRange(0, 95, 100),
    )


def test_from_offset_below_retention_starts_at_low() -> None:
    plan = plan_from_offset(OFFSETS, 0, start=2, limit=5)
    assert plan.ranges == (OffsetRange(0, 10, 15),)
    assert "no longer available" in plan.notes[0]


def test_from_offset_at_end_reads_nothing() -> None:
    plan = plan_from_offset(OFFSETS, 0, start=100, limit=5)
    assert plan.ranges == ()
    assert "nothing to read yet" in plan.notes[0]


def test_partition_offsets_message_count() -> None:
    assert PartitionOffsets(0, 10, 100).message_count == 90
    assert PartitionOffsets(0, 5, 5).message_count == 0
    assert OffsetRange(0, 9, 3).size == 0
