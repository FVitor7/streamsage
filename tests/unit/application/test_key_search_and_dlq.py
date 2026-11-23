import pytest

from streamsage.application.use_cases.find_messages_by_key import (
    MAX_SEARCH_KEY_BYTES,
    FindMessagesByKeyUseCase,
    KeySearchLimits,
)
from streamsage.application.use_cases.list_dlq_topics import (
    MAX_DLQ_TOPICS,
    ListDlqTopicsUseCase,
)
from streamsage.domain.entities.message import OffsetRange, PartitionOffsets
from streamsage.domain.exceptions import InvalidInputError, KafkaResourceNotFoundError
from streamsage.domain.messages.dlq import DlqTopicMatcher
from streamsage.domain.messages.sanitizer import PayloadTreatment, SanitizationPolicy
from tests.fakes import FakeMessageReader, FakeTopicReader, make_topic, raw_message


def _messages() -> list:
    # Keys are "k-<offset>"; offsets 3 and 8 of partition 0 share key "k-3".
    messages = [raw_message(0, o) for o in range(10)] + [
        raw_message(1, o) for o in range(4)
    ]
    messages[8] = raw_message(0, 8)
    object.__setattr__(messages[8], "key", b"k-3")
    return messages


def _use_case(
    reader: FakeMessageReader,
    per_partition: int = 1_000,
    total: int = 10_000,
) -> FindMessagesByKeyUseCase:
    return FindMessagesByKeyUseCase(
        reader,
        SanitizationPolicy(),
        KeySearchLimits(
            max_results=20, max_scan_per_partition=per_partition, max_scan_total=total
        ),
    )


def test_finds_matches_newest_first_and_covers_whole_topic() -> None:
    reader = FakeMessageReader(messages=_messages())
    result = _use_case(reader).execute("orders", "k-3", limit=5)
    assert [(m.partition, m.offset) for m in result.matches] == [(0, 8), (0, 3), (1, 3)]
    assert result.scanned == 14
    assert result.searched_whole_topic is True
    assert result.notes == ()
    assert all(m.payload_treatment is PayloadTreatment.OMITTED for m in result.matches)


def test_limit_keeps_only_the_newest_matches() -> None:
    reader = FakeMessageReader(messages=_messages())
    result = _use_case(reader).execute("orders", "k-3", limit=1)
    assert [(m.partition, m.offset) for m in result.matches] == [(0, 8)]


def test_partial_coverage_is_explained() -> None:
    reader = FakeMessageReader(messages=_messages())
    result = _use_case(reader, per_partition=4).execute("orders", "k-5", limit=5)
    assert result.matches == ()
    assert result.searched_whole_topic is False
    assert reader.requested_ranges == [(OffsetRange(0, 6, 10), OffsetRange(1, 0, 4))]
    assert "No match does not mean the key is absent" in result.notes[0]
    assert "partition(s) 0" in result.notes[0]


def test_incomplete_scan_is_flagged() -> None:
    reader = FakeMessageReader(messages=_messages(), complete=False)
    result = _use_case(reader).execute("orders", "k-3", limit=5)
    assert result.searched_whole_topic is False
    assert any("partial" in note for note in result.notes)


def test_search_one_partition() -> None:
    reader = FakeMessageReader(messages=_messages())
    result = _use_case(reader).execute("orders", "k-3", limit=5, partition=1)
    assert [(m.partition, m.offset) for m in result.matches] == [(1, 3)]


@pytest.mark.parametrize(
    ("key", "limit", "partition", "message"),
    [
        ("", 5, None, "key"),
        ("x" * (MAX_SEARCH_KEY_BYTES + 1), 5, None, "key"),
        ("ç" * (MAX_SEARCH_KEY_BYTES // 2 + 1), 5, None, "bytes"),
        ("k", 0, None, "limit"),
        ("k", 21, None, "limit"),
        ("k", 5, -1, "partition"),
    ],
)
def test_invalid_arguments(
    key: str, limit: int, partition: int | None, message: str
) -> None:
    reader = FakeMessageReader()
    with pytest.raises(InvalidInputError, match=message):
        _use_case(reader).execute("orders", key, limit, partition=partition)
    assert reader.requested_ranges == []


def test_missing_topic_and_invalid_limits() -> None:
    with pytest.raises(KafkaResourceNotFoundError):
        _use_case(FakeMessageReader()).execute("missing", "k", 1)
    with pytest.raises(InvalidInputError):
        KeySearchLimits(max_results=1, max_scan_per_partition=0, max_scan_total=1)


def _dlq_use_case(patterns: list[str], topic_names: list[str]) -> ListDlqTopicsUseCase:
    offsets = [PartitionOffsets(0, 10, 15), PartitionOffsets(1, 0, 2)]
    return ListDlqTopicsUseCase(
        FakeTopicReader([make_topic(name) for name in topic_names]),
        FakeMessageReader(offsets=offsets),
        DlqTopicMatcher(patterns),
    )


def test_unconfigured_dlq_says_so() -> None:
    overview = _dlq_use_case([], ["orders.DLQ"]).execute()
    assert overview.configured is False
    assert overview.topics == ()
    assert "does not assume a DLQ naming convention" in overview.notes[0]


def test_dlq_topics_are_matched_and_counted() -> None:
    overview = _dlq_use_case(
        ["*.DLQ", "dead-letter-*"],
        ["orders", "orders.DLQ", "dead-letter-pay", "__consumer_offsets"],
    ).execute()
    assert overview.configured is True
    assert [(t.name, t.partition_count, t.message_count) for t in overview.topics] == [
        ("dead-letter-pay", 2, 7),
        ("orders.DLQ", 2, 7),
    ]
    assert overview.notes == ()


def test_no_matching_dlq_topic() -> None:
    overview = _dlq_use_case(["*.DLQ"], ["orders"]).execute()
    assert overview.topics == ()
    assert overview.notes == ("No existing topic matches the configured DLQ patterns.",)


def test_dlq_summary_is_capped() -> None:
    names = [f"t{i:03d}.DLQ" for i in range(MAX_DLQ_TOPICS + 5)]
    overview = _dlq_use_case(["*.DLQ"], names).execute()
    assert len(overview.topics) == MAX_DLQ_TOPICS
    assert "only the first" in overview.notes[0]


def test_unavailable_partitions_prevent_whole_topic_claims() -> None:
    reader = FakeMessageReader(messages=_messages(), unavailable=(5,))
    result = _use_case(reader).execute("orders", "k-3", limit=5)
    assert result.searched_whole_topic is False
    assert result.unavailable_partitions == (5,)
    assert any("partition(s) 5 are unavailable" in note for note in result.notes)


def test_dlq_counts_note_unavailable_partitions() -> None:
    use_case = ListDlqTopicsUseCase(
        FakeTopicReader([make_topic("orders.DLQ")]),
        FakeMessageReader(offsets=[PartitionOffsets(0, 0, 4)], unavailable=(1,)),
        DlqTopicMatcher(["*.DLQ"]),
    )
    overview = use_case.execute()
    assert [(t.partition_count, t.message_count) for t in overview.topics] == [(2, 4)]
    assert "1 partition(s) are unavailable" in overview.notes[0]
