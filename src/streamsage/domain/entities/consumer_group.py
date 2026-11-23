"""Consumer group domain entities."""

from dataclasses import dataclass
from enum import StrEnum


class ConsumerGroupState(StrEnum):
    """Group state as reported by the group coordinator."""

    STABLE = "stable"
    EMPTY = "empty"
    PREPARING_REBALANCE = "preparing_rebalance"
    COMPLETING_REBALANCE = "completing_rebalance"
    DEAD = "dead"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True, order=True)
class TopicPartitionRef:
    topic: str
    partition: int


@dataclass(frozen=True, slots=True)
class ConsumerGroupSummary:
    """A group as returned by a listing (no member details)."""

    group_id: str
    state: ConsumerGroupState
    is_simple: bool


@dataclass(frozen=True, slots=True)
class ConsumerGroupListing:
    """Groups returned by a listing, and whether every broker answered.

    Kafka lists groups by asking every broker. If some fail, the groups that
    were returned are accurate but others may be missing: a group absent
    from an incomplete listing may still exist.
    """

    groups: tuple[ConsumerGroupSummary, ...]
    failed_brokers: int = 0
    failure_codes: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return self.failed_brokers == 0


@dataclass(frozen=True, slots=True)
class GroupMember:
    member_id: str
    client_id: str
    host: str
    group_instance_id: str | None
    assignment: tuple[TopicPartitionRef, ...]


@dataclass(frozen=True, slots=True)
class ConsumerGroup:
    """A described group.

    ``is_simple`` is true for groups that only store committed offsets and
    have no group protocol (for example offsets set by tooling).
    """

    group_id: str
    state: ConsumerGroupState
    is_simple: bool
    partition_assignor: str | None
    coordinator_id: int | None
    members: tuple[GroupMember, ...]

    @property
    def member_count(self) -> int:
        return len(self.members)

    @property
    def assigned_partitions(self) -> tuple[TopicPartitionRef, ...]:
        return tuple(
            sorted({tp for member in self.members for tp in member.assignment})
        )
