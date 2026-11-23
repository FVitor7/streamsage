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
    # Original payload size when ``value`` was not kept in full (see
    # ``value_truncated``); otherwise the size is ``len(value)``.
    original_value_size: int | None = None

    @property
    def value_size(self) -> int | None:
        if self.original_value_size is not None:
            return self.original_value_size
        return None if self.value is None else len(self.value)

    @property
    def value_truncated(self) -> bool:
        """True when the payload was too large to keep and was dropped."""
        return self.original_value_size is not None

    def __repr__(self) -> str:
        # Never render payload bytes, even in tracebacks or debug output.
        return (
            f"RawMessage(topic={self.topic!r}, partition={self.partition}, "
            f"offset={self.offset}, value_size={self.value_size})"
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
    messages_read: int = 0


@dataclass(frozen=True, slots=True)
class TopicOffsets:
    """Offsets of a topic's partitions.

    ``unavailable_partitions`` lists partitions whose offsets could not be
    read (for example while a partition has no leader); they cannot be read
    either, and callers must say so rather than silently skip them.
    """

    partitions: tuple[PartitionOffsets, ...]
    unavailable_partitions: tuple[int, ...] = ()

    @property
    def partition_count(self) -> int:
        return len(self.partitions) + len(self.unavailable_partitions)


@dataclass(frozen=True, slots=True)
class KeyScanResult:
    """Messages with a given key found while scanning bounded ranges.

    ``scanned`` counts every message read, matching or not. ``matches`` holds
    at most the requested number of newest matches per partition.
    """

    matches: tuple[RawMessage, ...]
    scanned: int
    complete: bool
