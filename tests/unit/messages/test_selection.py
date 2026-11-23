import random

from streamsage.domain.entities.message import RawMessage
from streamsage.domain.messages.selection import (
    NewestMessages,
    order_newest_first,
    uncertain_order_note,
)
from tests.fakes import raw_message
from tests.fakes import untimed_message as _untimed


def _ids(messages: tuple[RawMessage, ...]) -> list[tuple[int, int]]:
    return [(m.partition, m.offset) for m in messages]


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
    assert newest.newest_first() == order_newest_first(messages)[:10]


def test_fewer_messages_than_limit() -> None:
    newest = NewestMessages(limit=5)
    newest.offer(raw_message(0, 1))
    assert len(newest.newest_first()) == 1


def test_offset_order_wins_within_a_partition_even_if_timestamps_go_back() -> None:
    # Producers set CreateTime, so a later offset can carry an older timestamp.
    messages = [
        raw_message(0, 0, timestamp_ms=500),
        raw_message(0, 1, timestamp_ms=100),
        raw_message(1, 0, timestamp_ms=300),
    ]
    assert _ids(order_newest_first(messages)) == [(0, 1), (0, 0), (1, 0)]


def test_missing_timestamp_inherits_the_previous_one_in_its_partition() -> None:
    messages = [
        raw_message(0, 0, timestamp_ms=1_000),
        _untimed(0, 1),
        raw_message(1, 0, timestamp_ms=900),
        raw_message(1, 1, timestamp_ms=1_100),
    ]
    # (0, 1) is not treated as 1970: it ranks right after (0, 0)'s timestamp.
    assert _ids(order_newest_first(messages)) == [(1, 1), (0, 1), (0, 0), (1, 0)]


def test_messages_that_cannot_be_placed_in_time_rank_last_in_offset_order() -> None:
    messages = [
        _untimed(0, 0),
        _untimed(0, 1),
        raw_message(1, 0, timestamp_ms=5),
        _untimed(2, 7),
    ]
    assert _ids(order_newest_first(messages)) == [(1, 0), (2, 7), (0, 1), (0, 0)]


def test_all_messages_without_timestamps_keep_offset_order_per_partition() -> None:
    messages = [_untimed(p, o) for p in range(2) for o in range(3)]
    ordered = _ids(order_newest_first(messages))
    for partition in (0, 1):
        offsets = [o for p, o in ordered if p == partition]
        assert offsets == [2, 1, 0]


def test_heap_counts_messages_without_timestamps() -> None:
    newest = NewestMessages(limit=1)
    for message in [_untimed(0, 0), raw_message(0, 1), _untimed(1, 0)]:
        newest.offer(message)
    assert newest.without_timestamp == 2
    assert _ids(newest.newest_first()) == [(0, 1)]


def test_out_of_order_offers_use_their_own_timestamp() -> None:
    newest = NewestMessages(limit=3)
    newest.offer(raw_message(0, 5, timestamp_ms=900))
    newest.offer(raw_message(0, 2, timestamp_ms=100))  # older offset, late
    newest.offer(raw_message(1, 0, timestamp_ms=500))
    assert _ids(newest.newest_first()) == [(0, 5), (1, 0), (0, 2)]


def test_uncertain_order_note() -> None:
    note = uncertain_order_note(3)
    assert note.startswith("3 message(s) read have no timestamp")
    assert "estimate" in note
