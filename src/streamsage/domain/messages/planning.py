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
