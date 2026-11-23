import random

from streamsage.domain.messages.selection import NewestMessages, recency_key
from tests.fakes import raw_message


def test_keeps_only_the_newest_messages_with_bounded_size() -> None:
    newest = NewestMessages(limit=3)
    messages = [
        raw_message(p, o, timestamp_ms=o * 10 + p) for p in range(4) for o in range(50)
    ]
    random.Random(7).shuffle(messages)
    for message in messages:
        newest.offer(message)
        assert len(newest) <= 3  # never holds more than the limit
    assert newest.offered == 200
    assert [(m.partition, m.offset) for m in newest.newest_first()] == [
        (3, 49),
        (2, 49),
        (1, 49),
    ]


def test_matches_a_full_sort() -> None:
    messages = [
        raw_message(p, o, timestamp_ms=(o * 7) % 13)
        for p in range(3)
        for o in range(20)
    ]
    newest = NewestMessages(limit=10)
    for message in messages:
        newest.offer(message)
    expected = sorted(messages, key=recency_key, reverse=True)[:10]
    assert newest.newest_first() == tuple(expected)


def test_fewer_messages_than_limit() -> None:
    newest = NewestMessages(limit=5)
    newest.offer(raw_message(0, 1))
    assert len(newest.newest_first()) == 1
