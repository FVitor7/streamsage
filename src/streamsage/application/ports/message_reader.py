from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from streamsage.domain.entities.message import (
    MessageReadResult,
    OffsetRange,
    PartitionOffsets,
)


@runtime_checkable
class MessageReader(Protocol):
    """Bounded, read-only access to messages.

    Implementations must never join a consumer group, commit offsets or
    produce messages, and must never log message contents.
    """

    def partition_offsets(self, topic: str) -> list[PartitionOffsets]:
        """Current offsets of every partition, or ``KafkaResourceNotFoundError``."""
        ...

    def read_range(
        self, topic: str, ranges: Sequence[OffsetRange]
    ) -> MessageReadResult:
        """Read the given ranges (and nothing beyond them) within a timeout."""
        ...
