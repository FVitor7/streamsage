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


def test_total_budget_is_split_across_partitions() -> None:
    plan = plan_key_scan(OFFSETS, max_per_partition=1_000, max_total=300)
    assert plan.per_partition_limit == 100
    assert plan.ranges[0] == OffsetRange(0, 4_900, 5_000)


def test_budget_never_drops_below_one_message_per_partition() -> None:
    assert (
        plan_key_scan(OFFSETS, max_per_partition=10, max_total=1).per_partition_limit
        == 1
    )


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
