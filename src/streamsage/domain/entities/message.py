"""Kafka messages as read from a partition, before sanitization.

Raw messages carry payload bytes. They never leave the application layer:
use cases turn them into sanitized ``InspectedMessage`` objects, and nothing
logs them.
"""

from dataclasses import dataclass
from enum import StrEnum


class TimestampType(StrEnum):
    CREATE_TIME = "create_time"
    LOG_APPEND_TIME = "log_append_time"
    NOT_AVAILABLE = "not_available"


@dataclass(frozen=True, slots=True)
class RawHeader:
    key: str
    value: bytes | None


@dataclass(frozen=True, slots=True, repr=False)
class RawMessage:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    timestamp_type: TimestampType
    key: bytes | None
    value: bytes | None
    headers: tuple[RawHeader, ...] = ()

    def __repr__(self) -> str:
        # Never render payload bytes, even in tracebacks or debug output.
        size = None if self.value is None else len(self.value)
        return (
            f"RawMessage(topic={self.topic!r}, partition={self.partition}, "
            f"offset={self.offset}, value_size={size})"
        )


@dataclass(frozen=True, slots=True)
class PartitionOffsets:
    """Offsets bounding the messages currently stored in a partition.

    ``low`` is the first available offset (older messages were deleted by
    retention); ``high`` is the offset the next message will get.
    """

    partition: int
    low: int
    high: int

    @property
    def message_count(self) -> int:
        """Upper bound: compaction and transaction markers leave gaps."""
        return max(self.high - self.low, 0)


@dataclass(frozen=True, slots=True)
class OffsetRange:
    """Messages to read: ``start`` inclusive, ``stop`` exclusive."""

    partition: int
    start: int
    stop: int

    @property
    def size(self) -> int:
        return max(self.stop - self.start, 0)


@dataclass(frozen=True, slots=True)
class MessageReadResult:
    """Messages read from the requested ranges.

    ``complete`` is False when the read stopped (for example on timeout)
    before every range was fully read.
    """

    messages: tuple[RawMessage, ...]
    complete: bool
