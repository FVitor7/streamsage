import pytest

from streamsage.domain.entities.message import (
    OffsetRange,
    PartitionOffsets,
    TopicOffsets,
)
from streamsage.domain.exceptions import InvalidInputError, KafkaMetadataError
from streamsage.domain.messages.planning import (
    plan_from_offset,
    plan_latest,
    readable_partitions,
)

OFFSETS = [
    PartitionOffsets(1, low=0, high=3),
    PartitionOffsets(0, low=10, high=100),
    PartitionOffsets(2, low=5, high=5),  # empty
]


def test_latest_reads_newest_messages_of_each_partition() -> None:
    plan = plan_latest(OFFSETS, limit=5, max_read=1_000)
    assert plan.ranges == (OffsetRange(0, 95, 100), OffsetRange(1, 0, 3))
    assert plan.exact is True
    assert plan.planned_messages == 8
    assert plan.notes == ("Partition(s) 2 currently hold no messages.",)


def test_latest_on_one_partition() -> None:
    plan = plan_latest(OFFSETS, limit=200, max_read=1_000, partition=0)
    assert plan.ranges == (OffsetRange(0, 10, 100),)  # bounded by retention
    assert plan.exact is True


def test_unknown_partition_is_rejected() -> None:
    with pytest.raises(InvalidInputError, match=r"available: 0, 1, 2"):
        plan_latest(OFFSETS, limit=5, max_read=1_000, partition=9)


def _many(count: int, messages: int = 1_000) -> list[PartitionOffsets]:
    return [PartitionOffsets(p, 0, messages) for p in range(count)]


@pytest.mark.parametrize("partitions", [500, 5_000])
def test_read_budget_bounds_large_topics(partitions: int) -> None:
    plan = plan_latest(_many(partitions), limit=100, max_read=10_000)
    assert plan.planned_messages <= 10_000
    assert sum(r.size for r in plan.ranges) == plan.planned_messages
    assert plan.exact is False
    assert any("Read budget reached" in note for note in plan.notes)


def test_budget_that_fits_every_partition_keeps_the_result_exact() -> None:
    plan = plan_latest(_many(100), limit=100, max_read=10_000)
    assert plan.exact is True
    assert plan.per_partition == 100
    assert plan.planned_messages == 10_000


def test_small_partitions_do_not_cost_budget_they_cannot_use() -> None:
    # 200 partitions with 3 messages each: 600 messages fit in the budget.
    plan = plan_latest(_many(200, messages=3), limit=100, max_read=1_000)
    assert plan.exact is True
    assert plan.planned_messages == 600


def test_more_partitions_than_budget_reads_one_from_a_deterministic_subset() -> None:
    plan = plan_latest(_many(50), limit=10, max_read=20)
    assert plan.planned_messages == 20
    assert [r.partition for r in plan.ranges] == list(range(20))
    assert plan.uncovered_partitions == tuple(range(20, 50))
    assert plan.exact is False
    assert any("30 partition(s) were not read" in note for note in plan.notes)


def test_partial_budget_can_still_be_exact_for_short_partitions() -> None:
    offsets = [PartitionOffsets(0, 0, 1_000), PartitionOffsets(1, 0, 2)]
    plan = plan_latest(offsets, limit=100, max_read=60)
    # Partition 1 holds only 2 messages and is fully read; partition 0 is not.
    assert plan.exact is False
    assert plan.planned_messages <= 60


def test_read_budget_must_be_positive() -> None:
    with pytest.raises(InvalidInputError):
        plan_latest(OFFSETS, limit=5, max_read=0)


def test_unavailable_partitions_are_reported_not_skipped_silently() -> None:
    offsets = TopicOffsets((PartitionOffsets(0, 0, 10),), unavailable_partitions=(1, 2))
    readable, unavailable, notes = readable_partitions(offsets, None)
    assert [p.partition for p in readable] == [0]
    assert unavailable == (1, 2)
    assert "partition(s) 1, 2 are unavailable" in notes[0]
    with pytest.raises(KafkaMetadataError, match="partition 2"):
        readable_partitions(offsets, 2)
    assert readable_partitions(offsets, 0)[1:] == ((), ())


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
