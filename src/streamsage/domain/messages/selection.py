"""Selection of the newest messages with bounded memory."""

import heapq
from itertools import count

from streamsage.domain.entities.message import RawMessage

RecencyKey = tuple[int, int, int]


def recency_key(message: RawMessage) -> RecencyKey:
    """Ordering used for "newest": timestamp, then offset, then partition."""
    return (message.timestamp_ms or 0, message.offset, -message.partition)


class NewestMessages:
    """Keeps only the ``limit`` newest messages offered to it.

    Messages are offered one at a time while reading, so memory stays
    proportional to ``limit`` no matter how many messages are scanned.
    """

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._heap: list[tuple[RecencyKey, int, RawMessage]] = []
        self._sequence = count()  # tie-breaker; RawMessage is not orderable
        self.offered = 0

    def offer(self, message: RawMessage) -> None:
        self.offered += 1
        entry = (recency_key(message), next(self._sequence), message)
        if len(self._heap) < self._limit:
            heapq.heappush(self._heap, entry)
        elif entry[0] > self._heap[0][0]:
            heapq.heapreplace(self._heap, entry)

    def newest_first(self) -> tuple[RawMessage, ...]:
        return tuple(m for _, _, m in sorted(self._heap, reverse=True))

    def __len__(self) -> int:
        return len(self._heap)
