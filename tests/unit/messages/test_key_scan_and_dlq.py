import pytest

from streamsage.domain.entities.message import OffsetRange, PartitionOffsets
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.messages.dlq import DlqTopicMatcher
from streamsage.domain.messages.planning import plan_key_scan

OFFSETS = [
    PartitionOffsets(0, low=0, high=5_000),
    PartitionOffsets(1, low=100, high=400),
    PartitionOffsets(2, low=7, high=7),
]


def test_key_scan_covers_newest_messages_within_per_partition_budget() -> None:
    plan = plan_key_scan(OFFSETS, max_per_partition=1_000, max_total=10_000)
    assert plan.per_partition_limit == 1_000
    assert plan.ranges == (OffsetRange(0, 4_000, 5_000), OffsetRange(1, 100, 400))
    coverage = {c.partition: c for c in plan.coverage}
    assert (coverage[0].scanned_count, coverage[0].reaches_start) == (1_000, False)
    assert (coverage[1].scanned_count, coverage[1].reaches_start) == (300, True)
    assert (coverage[2].scanned_count, coverage[2].reaches_start) == (0, True)


def test_total_budget_is_split_across_non_empty_partitions() -> None:
    plan = plan_key_scan(OFFSETS, max_per_partition=1_000, max_total=300)
    # Partition 2 is empty, so the budget is split between partitions 0 and 1.
    assert plan.per_partition_limit == 150
    assert plan.ranges == (OffsetRange(0, 4_850, 5_000), OffsetRange(1, 250, 400))
    assert plan.uncovered_partitions == ()


def test_total_budget_is_a_hard_limit_with_more_partitions_than_budget() -> None:
    offsets = [PartitionOffsets(p, low=0, high=50) for p in range(500)]
    plan = plan_key_scan(offsets, max_per_partition=1_000, max_total=10)
    assert plan.per_partition_limit == 1
    assert plan.planned_messages == 10
    assert [r.partition for r in plan.ranges] == list(range(10))
    assert plan.uncovered_partitions == tuple(range(10, 500))
    assert [c.partition for c in plan.coverage] == list(range(10))


def test_uncovered_partitions_are_chosen_deterministically() -> None:
    offsets = [PartitionOffsets(p, low=0, high=5) for p in (7, 3, 9, 1)]
    plan = plan_key_scan(offsets, max_per_partition=100, max_total=2)
    assert [r.partition for r in plan.ranges] == [1, 3]
    assert plan.uncovered_partitions == (7, 9)


@pytest.mark.parametrize("partitions", [1, 3, 99, 100, 101, 1_000, 5_000])
@pytest.mark.parametrize("max_total", [1, 7, 100, 10_000])
def test_planned_messages_never_exceed_total_budget(
    partitions: int, max_total: int
) -> None:
    offsets = [PartitionOffsets(p, low=0, high=1_000) for p in range(partitions)]
    plan = plan_key_scan(offsets, max_per_partition=1_000, max_total=max_total)
    assert plan.planned_messages <= max_total
    covered = {c.partition for c in plan.coverage}
    assert covered.isdisjoint(plan.uncovered_partitions)
    assert len(covered) + len(plan.uncovered_partitions) == partitions


def test_empty_partitions_are_covered_without_using_the_budget() -> None:
    offsets = [PartitionOffsets(p, low=4, high=4) for p in range(5)]
    offsets.append(PartitionOffsets(5, low=0, high=10))
    plan = plan_key_scan(offsets, max_per_partition=100, max_total=3)
    assert plan.ranges == (OffsetRange(5, 7, 10),)
    assert plan.uncovered_partitions == ()
    assert all(c.reaches_start for c in plan.coverage if c.partition != 5)


def test_budgets_must_be_positive() -> None:
    with pytest.raises(InvalidInputError):
        plan_key_scan(OFFSETS, max_per_partition=0, max_total=10)
    with pytest.raises(InvalidInputError):
        plan_key_scan(OFFSETS, max_per_partition=10, max_total=0)


def test_key_scan_on_one_partition_gets_the_whole_budget() -> None:
    plan = plan_key_scan(OFFSETS, max_per_partition=1_000, max_total=600, partition=0)
    assert plan.ranges == (OffsetRange(0, 4_400, 5_000),)
    with pytest.raises(InvalidInputError):
        plan_key_scan(OFFSETS, 10, 10, partition=9)


@pytest.mark.parametrize(
    ("topic", "expected"),
    [
        ("orders.DLQ", True),
        ("payments.DLQ", True),
        ("orders.dlq", False),  # case-sensitive, like Kafka topic names
        ("dead-letter-orders", True),
        ("billing-failures", True),
        ("orders", False),
    ],
)
def test_dlq_matching(topic: str, expected: bool) -> None:
    matcher = DlqTopicMatcher(["*.DLQ", " dead-letter-* ", "billing-failures", ""])
    assert matcher.matches(topic) is expected
    assert matcher.patterns == ("*.DLQ", "dead-letter-*", "billing-failures")
    assert matcher.configured is True


def test_unconfigured_matcher_matches_nothing() -> None:
    matcher = DlqTopicMatcher()
    assert matcher.configured is False
    assert matcher.matches("orders.DLQ") is False


@pytest.mark.parametrize("pattern", ["orders dlq", "orders/dlq", "bad;name"])
def test_invalid_dlq_patterns(pattern: str) -> None:
    with pytest.raises(InvalidInputError, match="Invalid DLQ topic pattern"):
        DlqTopicMatcher([pattern])
