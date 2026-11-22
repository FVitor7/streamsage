"""Consumer lag value objects.

Missing information is never turned into zero: when the committed offset or
the log end offset of a partition is unknown, its lag is ``None``.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PartitionLag:
    """Lag of one consumer group on one partition.

    ``log_end_offset`` is the high watermark at the time it was read. ``lag``
    is ``None`` when either offset is unknown. A committed offset ahead of
    the read end offset (possible because both are read at slightly
    different moments) yields a lag of 0 rather than a negative value.
    """

    topic: str
    partition: int
    committed_offset: int | None
    log_end_offset: int | None

    @property
    def lag(self) -> int | None:
        if self.committed_offset is None or self.log_end_offset is None:
            return None
        return max(self.log_end_offset - self.committed_offset, 0)


@dataclass(frozen=True, slots=True)
class ConsumerGroupLag:
    """Lag of a group across partitions, ordered by lag (largest first).

    Partitions with unknown lag are listed last and are excluded from
    ``total_lag``; ``unknown_lag_partition_count`` makes that explicit.
    """

    group_id: str
    partitions: tuple[PartitionLag, ...]

    def __post_init__(self) -> None:
        ordered = tuple(sorted(self.partitions, key=_lag_order))
        object.__setattr__(self, "partitions", ordered)

    @property
    def total_lag(self) -> int | None:
        """Sum of known lags; ``None`` if no partition has a known lag."""
        known = [p.lag for p in self.partitions if p.lag is not None]
        return sum(known) if known else None

    @property
    def max_lag(self) -> int | None:
        known = [p.lag for p in self.partitions if p.lag is not None]
        return max(known) if known else None

    @property
    def unknown_lag_partition_count(self) -> int:
        return sum(1 for p in self.partitions if p.lag is None)


def _lag_order(partition: PartitionLag) -> tuple[bool, int, str, int]:
    lag = partition.lag
    return (lag is None, -(lag or 0), partition.topic, partition.partition)
