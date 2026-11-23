"""Selection of the newest messages with bounded memory.

How "newest" is decided across partitions:

- Within a partition, the offset is the order: a higher offset is always
  newer, even if its timestamp is lower (producers set ``CreateTime``, so
  timestamps are not guaranteed to grow).
- Across partitions, only timestamps can be compared. Each message is ranked
  by its *effective timestamp*: the highest timestamp seen in its partition
  up to and including that message. This keeps offset order within a
  partition and gives a message without a timestamp the bound of the
  messages before it, instead of pretending it is from 1970.
- A message without a timestamp, and with no earlier timestamp in its
  partition, cannot be placed in time. It ranks below every message that
  can, still in offset order within its partition. Callers report that the
  order across partitions is then uncertain.
"""

import heapq
from dataclasses import dataclass
from itertools import count

from streamsage.domain.entities.message import RawMessage

# (placed in time, effective timestamp, offset, -partition)
RecencyKey = tuple[bool, int, int, int]


@dataclass(slots=True)
class _PartitionCursor:
    last_offset: int
    max_timestamp_ms: int | None


class RecencyRanker:
    """Computes recency keys for messages offered in offset order.

    Messages of one partition must be offered in ascending offset order (the
    order Kafka delivers them). A message that arrives out of order is
    ranked by its own timestamp only.
    """

    def __init__(self) -> None:
        self._cursors: dict[int, _PartitionCursor] = {}
        self.without_timestamp = 0

    def key(self, message: RawMessage) -> RecencyKey:
        if message.timestamp_ms is None:
            self.without_timestamp += 1
        cursor = self._cursors.get(message.partition)
        inherited = None
        if cursor is not None and message.offset > cursor.last_offset:
            inherited = cursor.max_timestamp_ms
        effective = _max(message.timestamp_ms, inherited)
        if cursor is None or message.offset > cursor.last_offset:
            self._cursors[message.partition] = _PartitionCursor(
                message.offset, effective
            )
        return (
            effective is not None,
            effective or 0,
            message.offset,
            -message.partition,
        )


def _max(a: int | None, b: int | None) -> int | None:
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


def order_newest_first(messages: list[RawMessage]) -> tuple[RawMessage, ...]:
    """All ``messages``, newest first, by the rules in this module."""
    ranker = RecencyRanker()
    in_log_order = sorted(messages, key=lambda m: (m.partition, m.offset))
    ranked = [(ranker.key(m), i, m) for i, m in enumerate(in_log_order)]
    return tuple(m for _, _, m in sorted(ranked, reverse=True))


class NewestMessages:
    """Keeps only the ``limit`` newest messages offered to it.

    Messages are offered one at a time while reading, so memory stays
    proportional to ``limit`` no matter how many messages are scanned.
    """

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._ranker = RecencyRanker()
        self._heap: list[tuple[RecencyKey, int, RawMessage]] = []
        self._sequence = count()  # tie-breaker; RawMessage is not orderable
        self.offered = 0

    @property
    def without_timestamp(self) -> int:
        """How many offered messages had no timestamp."""
        return self._ranker.without_timestamp

    def offer(self, message: RawMessage) -> None:
        self.offered += 1
        entry = (self._ranker.key(message), next(self._sequence), message)
        if len(self._heap) < self._limit:
            heapq.heappush(self._heap, entry)
        elif entry[0] > self._heap[0][0]:
            heapq.heapreplace(self._heap, entry)

    def newest_first(self) -> tuple[RawMessage, ...]:
        return tuple(m for _, _, m in sorted(self._heap, reverse=True))

    def __len__(self) -> int:
        return len(self._heap)


def uncertain_order_note(without_timestamp: int) -> str:
    return (
        f"{without_timestamp} message(s) read have no timestamp. Within each "
        "partition, messages are in offset order. Across partitions, a message "
        "without a timestamp is ranked as if it had the nearest earlier "
        "timestamp in its partition, or last if there is none, so the order "
        "across partitions is an estimate."
    )
