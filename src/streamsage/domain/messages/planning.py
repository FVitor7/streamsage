"""Which offsets to read, given what each partition currently holds."""

from collections.abc import Sequence
from dataclasses import dataclass

from streamsage.domain.entities.message import (
    OffsetRange,
    PartitionOffsets,
    TopicOffsets,
)
from streamsage.domain.exceptions import InvalidInputError, KafkaMetadataError


@dataclass(frozen=True, slots=True)
class ReadPlan:
    ranges: tuple[OffsetRange, ...]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class LatestPlan:
    """Plan for "the newest ``limit`` messages" under a read budget.

    The globally newest ``limit`` messages can sit in any partition, so an
    exact answer requires reading the newest ``min(limit, available)``
    messages of *every* partition. When that exceeds ``max_read``, fewer
    messages are read per partition and ``exact`` is False: the answer is
    the newest messages *among those read*.
    """

    ranges: tuple[OffsetRange, ...]
    exact: bool
    per_partition: int
    planned_messages: int
    uncovered_partitions: tuple[int, ...] = ()
    notes: tuple[str, ...] = ()


def plan_latest(
    offsets: Sequence[PartitionOffsets],
    limit: int,
    max_read: int,
    partition: int | None = None,
) -> LatestPlan:
    """Read the newest messages of each selected partition, within ``max_read``."""
    if max_read < 1:
        raise InvalidInputError("max_read must be at least 1.")
    selected = _select(offsets, partition)
    non_empty = [p for p in selected if p.high > p.low]
    notes = []
    empty = [p.partition for p in selected if p.high <= p.low]
    if empty:
        notes.append(f"Partition(s) {_list(empty)} currently hold no messages.")

    wanted = {p.partition: min(limit, p.message_count) for p in non_empty}
    if sum(wanted.values()) <= max_read:
        ranges = tuple(
            OffsetRange(p.partition, p.high - wanted[p.partition], p.high)
            for p in non_empty
        )
        return LatestPlan(ranges, True, limit, sum(wanted.values()), notes=tuple(notes))

    # Over budget: spread it evenly. If there are more partitions than
    # messages in the budget, read one message from the first ``max_read``
    # partitions (lowest ids, so the choice is deterministic).
    per_partition = max(max_read // len(non_empty), 1)
    covered = non_empty[: max_read // per_partition]
    uncovered = tuple(p.partition for p in non_empty[len(covered) :])
    ranges = tuple(
        OffsetRange(p.partition, max(p.high - per_partition, p.low), p.high)
        for p in covered
    )
    planned = sum(r.size for r in ranges)
    exact = not uncovered and all(r.size >= wanted[r.partition] for r in ranges)
    if not exact:
        notes.append(
            f"Read budget reached: read up to {per_partition} newest message(s) "
            f"from each of {len(covered)} partition(s) instead of {limit}, so "
            "these are the newest messages among those read, not necessarily "
            "the newest in the topic."
        )
    if uncovered:
        notes.append(
            f"{len(uncovered)} partition(s) were not read at all because the "
            f"read budget ({max_read} messages) is smaller than the number of "
            "partitions."
        )
    return LatestPlan(ranges, exact, per_partition, planned, uncovered, tuple(notes))


def plan_from_offset(
    offsets: Sequence[PartitionOffsets], partition: int, start: int, limit: int
) -> ReadPlan:
    """Up to ``limit`` messages of one partition, starting at ``start``."""
    (target,) = _select(offsets, partition)
    notes = []
    begin = start
    if start < target.low:
        begin = target.low
        notes.append(
            f"Offset {start} is no longer available (deleted by retention or "
            f"compaction); reading from the first available offset {target.low}."
        )
    if begin >= target.high:
        notes.append(
            f"Offset {start} is at or beyond the end of partition {partition} "
            f"(next offset {target.high}); there is nothing to read yet."
        )
        return ReadPlan((), tuple(notes))
    return ReadPlan(
        (OffsetRange(partition, begin, min(begin + limit, target.high)),),
        tuple(notes),
    )


def _select(
    offsets: Sequence[PartitionOffsets], partition: int | None
) -> list[PartitionOffsets]:
    if partition is None:
        return sorted(offsets, key=lambda p: p.partition)
    selected = [p for p in offsets if p.partition == partition]
    if not selected:
        available = _list([p.partition for p in offsets])
        raise InvalidInputError(
            f"Partition {partition} does not exist (available: {available})."
        )
    return selected


def _list(partitions: Sequence[int]) -> str:
    return ", ".join(str(p) for p in sorted(partitions)) or "none"


@dataclass(frozen=True, slots=True)
class ScanCoverage:
    """What part of a partition a bounded scan covers."""

    partition: int
    from_offset: int
    to_offset: int  # exclusive
    first_available_offset: int

    @property
    def scanned_count(self) -> int:
        return max(self.to_offset - self.from_offset, 0)

    @property
    def reaches_start(self) -> bool:
        """True if every message currently in the partition is covered."""
        return self.from_offset <= self.first_available_offset


@dataclass(frozen=True, slots=True)
class ScanPlan:
    ranges: tuple[OffsetRange, ...]
    coverage: tuple[ScanCoverage, ...]
    per_partition_limit: int
    uncovered_partitions: tuple[int, ...] = ()

    @property
    def planned_messages(self) -> int:
        return sum(r.size for r in self.ranges)


def plan_key_scan(
    offsets: Sequence[PartitionOffsets],
    max_per_partition: int,
    max_total: int,
    partition: int | None = None,
) -> ScanPlan:
    """Scan the newest messages of each partition, within both budgets.

    ``max_total`` is a hard limit: the plan never covers more messages. It is
    split evenly across the non-empty partitions, so a topic with many
    partitions scans fewer messages per partition. When there are more
    non-empty partitions than ``max_total``, one message is scanned from
    each of the first ``max_total`` partitions (lowest ids, so the choice is
    deterministic) and the rest are reported as uncovered.
    """
    if max_per_partition < 1 or max_total < 1:
        raise InvalidInputError("Key search budgets must be at least 1.")
    selected = _select(offsets, partition)
    non_empty = [p for p in selected if p.high > p.low]
    per_partition = min(max_per_partition, max_total // max(len(non_empty), 1))
    covered = non_empty
    if per_partition < 1:
        per_partition = 1
        covered = non_empty[:max_total]
    covered_ids = {p.partition for p in covered}
    uncovered = tuple(p.partition for p in non_empty if p.partition not in covered_ids)
    coverage = tuple(
        ScanCoverage(
            partition=p.partition,
            from_offset=max(p.high - per_partition, p.low),
            to_offset=p.high,
            first_available_offset=p.low,
        )
        for p in selected
        if p.partition not in uncovered
    )
    ranges = tuple(
        OffsetRange(c.partition, c.from_offset, c.to_offset)
        for c in coverage
        if c.scanned_count > 0
    )
    return ScanPlan(ranges, coverage, per_partition, uncovered)


def readable_partitions(
    offsets: TopicOffsets, partition: int | None
) -> tuple[list[PartitionOffsets], tuple[int, ...], tuple[str, ...]]:
    """Partitions that can be read, the unavailable ones, and notes about them.

    Asking for one specific partition whose offsets are unavailable is an
    error; otherwise unavailable partitions are reported, never silently
    skipped.
    """
    if partition is not None and partition in offsets.unavailable_partitions:
        raise KafkaMetadataError(
            f"Offsets of partition {partition} are unavailable (for example, it "
            "has no leader right now); it cannot be read."
        )
    unavailable = () if partition is not None else offsets.unavailable_partitions
    notes: tuple[str, ...] = ()
    if unavailable:
        notes = (
            f"Offsets of partition(s) {_list(unavailable)} are unavailable (for "
            "example, no leader right now); they were not read.",
        )
    return list(offsets.partitions), unavailable, notes
