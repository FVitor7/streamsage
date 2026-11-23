import pytest

from streamsage.application.use_cases.inspect_messages import InspectMessagesUseCase
from streamsage.domain.entities.message import OffsetRange, PartitionOffsets
from streamsage.domain.exceptions import InvalidInputError, KafkaResourceNotFoundError
from streamsage.domain.messages.sanitizer import PayloadTreatment, SanitizationPolicy
from tests.fakes import FakeMessageReader, raw_message, untimed_message


def _use_case(
    reader: FakeMessageReader, payloads: bool = False, max_messages: int = 20
) -> InspectMessagesUseCase:
    return InspectMessagesUseCase(
        reader, SanitizationPolicy(payloads_enabled=payloads), max_messages
    )


def test_latest_messages_newest_first_across_partitions() -> None:
    messages = [
        raw_message(0, 8, timestamp_ms=100),
        raw_message(0, 9, timestamp_ms=300),
        raw_message(1, 3, timestamp_ms=200),
    ]
    reader = FakeMessageReader(messages=messages)
    result = _use_case(reader).execute("orders", limit=2)
    assert [(m.partition, m.offset) for m in result.messages] == [(0, 9), (1, 3)]
    assert reader.requested_ranges == [(OffsetRange(0, 8, 10), OffsetRange(1, 2, 4))]
    assert result.complete is True
    assert result.payloads_included is False
    assert all(m.payload_treatment is PayloadTreatment.OMITTED for m in result.messages)


def test_read_forward_from_offset_in_offset_order() -> None:
    reader = FakeMessageReader()
    result = _use_case(reader).execute("orders", limit=3, partition=0, start_offset=4)
    assert [m.offset for m in result.messages] == [4, 5, 6]
    assert reader.requested_ranges == [(OffsetRange(0, 4, 7),)]


def test_payloads_follow_the_policy() -> None:
    result = _use_case(FakeMessageReader(), payloads=True).execute("orders", limit=1)
    assert result.payloads_included is True
    assert result.messages[0].payload_treatment is PayloadTreatment.JSON_REDACTED


def test_partial_reads_are_flagged() -> None:
    result = _use_case(FakeMessageReader(complete=False)).execute("orders", limit=5)
    assert result.complete is False
    assert any("partial" in note for note in result.notes)


def test_planning_notes_are_returned() -> None:
    reader = FakeMessageReader(offsets=[PartitionOffsets(0, 0, 0)], messages=[])
    result = _use_case(reader).execute("orders", limit=5)
    assert result.messages == ()
    assert result.notes == ("Partition(s) 0 currently hold no messages.",)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"limit": 0}, "limit"),
        ({"limit": 21}, "limit"),
        ({"limit": 5, "partition": -1}, "partition"),
        ({"limit": 5, "start_offset": 3}, "requires a partition"),
        ({"limit": 5, "partition": 0, "start_offset": -1}, "start_offset"),
    ],
)
def test_invalid_arguments_are_rejected_before_reading(
    kwargs: dict[str, int], message: str
) -> None:
    reader = FakeMessageReader()
    with pytest.raises(InvalidInputError, match=message):
        _use_case(reader).execute("orders", **kwargs)
    assert reader.requested_ranges == []


def test_invalid_topic_name_and_missing_topic() -> None:
    with pytest.raises(InvalidInputError):
        _use_case(FakeMessageReader()).execute("bad name", limit=1)
    with pytest.raises(KafkaResourceNotFoundError):
        _use_case(FakeMessageReader()).execute("missing", limit=1)


def test_newest_messages_are_exact_within_the_read_budget() -> None:
    result = _use_case(FakeMessageReader()).execute("orders", limit=3)
    assert result.exact is True
    assert result.messages_read == 6  # 3 newest of each of the 2 partitions


def test_read_budget_limits_reads_and_marks_the_result_inexact() -> None:
    offsets = [PartitionOffsets(p, 0, 50) for p in range(10)]
    messages = [raw_message(p, o, timestamp_ms=o) for p in range(10) for o in range(50)]
    reader = FakeMessageReader(offsets=offsets, messages=messages)
    use_case = InspectMessagesUseCase(
        reader, SanitizationPolicy(), max_messages=20, max_read=40
    )
    result = use_case.execute("orders", limit=20)
    assert result.messages_read == 40  # 4 per partition instead of 20
    assert len(result.messages) == 20
    assert result.exact is False
    assert any("Read budget reached" in note for note in result.notes)


def test_unavailable_partitions_make_the_result_inexact() -> None:
    reader = FakeMessageReader(unavailable=(7,))
    result = _use_case(reader).execute("orders", limit=2)
    assert result.exact is False
    assert any("partition(s) 7 are unavailable" in note for note in result.notes)


def test_requesting_an_unavailable_partition_fails_clearly() -> None:
    from streamsage.domain.exceptions import KafkaMetadataError

    reader = FakeMessageReader(unavailable=(7,))
    with pytest.raises(KafkaMetadataError, match="partition 7"):
        _use_case(reader).execute("orders", limit=2, partition=7)


def test_reading_from_an_offset_is_exact() -> None:
    result = _use_case(FakeMessageReader()).execute(
        "orders", limit=3, partition=0, start_offset=4
    )
    assert result.exact is True
    assert result.messages_read == 3


def test_messages_without_timestamps_make_cross_partition_order_inexact() -> None:
    offsets = [PartitionOffsets(0, 0, 2), PartitionOffsets(1, 0, 2)]
    messages = [
        raw_message(0, 0),
        untimed_message(0, 1),
        raw_message(1, 0),
        raw_message(1, 1),
    ]
    reader = FakeMessageReader(offsets=offsets, messages=messages)
    result = _use_case(reader).execute("orders", limit=4)
    assert result.exact is False
    assert any("1 message(s) read have no timestamp" in n for n in result.notes)
    # Offset order is preserved within partition 0.
    partition_0 = [m.offset for m in result.messages if m.partition == 0]
    assert partition_0 == [1, 0]


def test_messages_without_timestamps_in_one_partition_stay_exact() -> None:
    offsets = [PartitionOffsets(0, 0, 3)]
    messages = [untimed_message(0, o) for o in range(3)]
    reader = FakeMessageReader(offsets=offsets, messages=messages)
    result = _use_case(reader).execute("orders", limit=2)
    assert result.exact is True
    assert [m.offset for m in result.messages] == [2, 1]
    assert result.notes == ()
