"""Topic-level domain entities."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Partition:
    """A topic partition as reported by cluster metadata.

    ``leader`` is ``None`` when the partition currently has no leader.
    ``metadata_error`` carries a normalized error code reported for this
    partition (for example ``LEADER_NOT_AVAILABLE``), if any.
    """

    partition_id: int
    leader: int | None
    replicas: tuple[int, ...]
    isr: tuple[int, ...]
    metadata_error: str | None = None

    @property
    def has_leader(self) -> bool:
        return self.leader is not None

    @property
    def is_under_replicated(self) -> bool:
        """True when at least one assigned replica is not in sync."""
        return len(self.isr) < len(self.replicas)


@dataclass(frozen=True, slots=True)
class Topic:
    name: str
    partitions: tuple[Partition, ...]
    internal: bool = False
    metadata_error: str | None = None

    @property
    def partition_count(self) -> int:
        return len(self.partitions)

    @property
    def replication_factor(self) -> int | None:
        """Largest replica set across partitions; ``None`` without partitions."""
        if not self.partitions:
            return None
        return max(len(partition.replicas) for partition in self.partitions)

    @property
    def under_replicated_partitions(self) -> tuple[Partition, ...]:
        return tuple(p for p in self.partitions if p.is_under_replicated)

    @property
    def leaderless_partitions(self) -> tuple[Partition, ...]:
        return tuple(p for p in self.partitions if not p.has_leader)
