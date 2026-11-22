"""Which offsets to read, given what each partition currently holds."""

from collections.abc import Sequence
from dataclasses import dataclass

from streamsage.domain.entities.message import OffsetRange, PartitionOffsets
from streamsage.domain.exceptions import InvalidInputError


@dataclass(frozen=True, slots=True)
class ReadPlan:
    ranges: tuple[OffsetRange, ...]
    notes: tuple[str, ...] = ()


def plan_latest(
    offsets: Sequence[PartitionOffsets], limit: int, partition: int | None = None
) -> ReadPlan:
    """The newest ``limit`` messages of each selected partition."""
    selected = _select(offsets, partition)
    ranges = tuple(
        OffsetRange(p.partition, max(p.high - limit, p.low), p.high)
        for p in selected
        if p.high > p.low
    )
    notes = []
    empty = [p.partition for p in selected if p.high <= p.low]
    if empty:
        notes.append(f"Partition(s) {_list(empty)} currently hold no messages.")
    return ReadPlan(ranges, tuple(notes))


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


def plan_key_scan(
    offsets: Sequence[PartitionOffsets],
    max_per_partition: int,
    max_total: int,
    partition: int | None = None,
) -> ScanPlan:
    """Scan the newest messages of each partition, within both budgets.

    The total budget is split evenly across the selected partitions, so a
    topic with many partitions scans fewer messages per partition.
    """
    selected = _select(offsets, partition)
    per_partition = max(1, min(max_per_partition, max_total // max(len(selected), 1)))
    coverage = tuple(
        ScanCoverage(
            partition=p.partition,
            from_offset=max(p.high - per_partition, p.low),
            to_offset=p.high,
            first_available_offset=p.low,
        )
        for p in selected
    )
    ranges = tuple(
        OffsetRange(c.partition, c.from_offset, c.to_offset)
        for c in coverage
        if c.scanned_count > 0
    )
    return ScanPlan(ranges, coverage, per_partition)
