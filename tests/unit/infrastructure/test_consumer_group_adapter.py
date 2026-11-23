"""Consumer group adapter tests with real confluent-kafka result classes."""

import concurrent.futures
from collections.abc import Mapping
from typing import Any

import pytest
from confluent_kafka import (
    ConsumerGroupState,
    ConsumerGroupTopicPartitions,
    KafkaError,
    KafkaException,
    Node,
    TopicPartition,
)
from confluent_kafka.admin._group import (
    ConsumerGroupDescription,
    ConsumerGroupListing,
    ListConsumerGroupsResult,
    MemberAssignment,
    MemberDescription,
)
from confluent_kafka.admin._listoffsets import ListOffsetsResultInfo

from streamsage.domain.entities.consumer_group import ConsumerGroupState as State
from streamsage.domain.entities.consumer_group import TopicPartitionRef
from streamsage.domain.exceptions import (
    KafkaConnectionError,
    KafkaMetadataError,
    KafkaResourceNotFoundError,
)
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway
from streamsage.infrastructure.kafka.kafka_consumer_group_adapter import (
    KafkaConsumerGroupAdapter,
)

Outcome = Any  # a result object, or an Exception to fail the future with


def _future(outcome: Outcome) -> concurrent.futures.Future[Any]:
    future: concurrent.futures.Future[Any] = concurrent.futures.Future()
    if isinstance(outcome, Exception):
        future.set_exception(outcome)
    else:
        future.set_result(outcome)
    return future


def _kafka_error(code: int) -> KafkaException:
    return KafkaException(KafkaError(code))


def _description(
    group_id: str,
    state: ConsumerGroupState,
    members: list[MemberDescription] | None = None,
) -> ConsumerGroupDescription:
    return ConsumerGroupDescription(
        group_id,
        not members,
        members or [],
        "range" if members else "",
        state,
        Node(1, "broker-1", 9092),
    )


def _member(member_id: str, *partitions: tuple[str, int]) -> MemberDescription:
    return MemberDescription(
        member_id,
        f"client-{member_id}",
        "/10.0.0.1",
        MemberAssignment([TopicPartition(t, p) for t, p in partitions]),
    )


class FakeAdminClient:
    def __init__(
        self,
        *,
        listing: Outcome = None,
        descriptions: Mapping[str, Outcome] | None = None,
        committed: Mapping[str, Outcome] | None = None,
        end_offsets: Mapping[tuple[str, int], Outcome] | None = None,
    ) -> None:
        self.listing = listing
        self.descriptions = dict(descriptions or {})
        self.committed = dict(committed or {})
        self.end_offsets = dict(end_offsets or {})
        self.offset_requests: list[list[tuple[str, int]]] = []
        self.timeouts: list[float] = []
        self.polls = 0

    def poll(self, timeout: float) -> int:
        self.polls += 1
        return 0

    def list_consumer_groups(self, **kwargs: Any) -> concurrent.futures.Future[Any]:
        self.timeouts.append(kwargs["request_timeout"])
        return _future(self.listing)

    def describe_consumer_groups(
        self, group_ids: list[str], **kwargs: Any
    ) -> dict[str, concurrent.futures.Future[Any]]:
        self.timeouts.append(kwargs["request_timeout"])
        return {
            g: _future(
                self.descriptions.get(g, _description(g, ConsumerGroupState.DEAD))
            )
            for g in group_ids
        }

    def list_consumer_group_offsets(
        self, requests: list[ConsumerGroupTopicPartitions], **kwargs: Any
    ) -> dict[str, concurrent.futures.Future[Any]]:
        self.timeouts.append(kwargs["request_timeout"])
        return {
            r.group_id: _future(
                self.committed.get(
                    r.group_id, ConsumerGroupTopicPartitions(r.group_id, [])
                )
            )
            for r in requests
        }

    def list_offsets(
        self, requests: dict[TopicPartition, Any], **kwargs: Any
    ) -> dict[TopicPartition, concurrent.futures.Future[Any]]:
        self.timeouts.append(kwargs["request_timeout"])
        self.offset_requests.append(sorted((tp.topic, tp.partition) for tp in requests))
        return {
            tp: _future(
                self.end_offsets.get(
                    (tp.topic, tp.partition),
                    _kafka_error(KafkaError._UNKNOWN_PARTITION),
                )
            )
            for tp in requests
        }


def _adapter(admin: FakeAdminClient) -> KafkaConsumerGroupAdapter:
    gateway = KafkaAdminGateway(
        admin,  # type: ignore[arg-type]
        timeout_seconds=6,
        error_events=KafkaErrorEvents(),
    )
    return KafkaConsumerGroupAdapter(gateway)


def _end(offset: int) -> ListOffsetsResultInfo:
    return ListOffsetsResultInfo(offset, -1, 0)


def test_list_consumer_groups_maps_states() -> None:
    admin = FakeAdminClient(
        listing=ListConsumerGroupsResult(
            valid=[
                ConsumerGroupListing("a", False, ConsumerGroupState.STABLE),
                ConsumerGroupListing("b", True, ConsumerGroupState.EMPTY),
                ConsumerGroupListing(
                    "c", False, ConsumerGroupState.PREPARING_REBALANCING
                ),
                ConsumerGroupListing(
                    "d", False, ConsumerGroupState.COMPLETING_REBALANCING
                ),
                ConsumerGroupListing("e", False, ConsumerGroupState.UNKNOWN),
                ConsumerGroupListing("f", False, None),
            ]
        )
    )
    listing = _adapter(admin).list_consumer_groups()
    assert listing.complete is True and listing.failure_codes == ()
    groups = {g.group_id: g for g in listing.groups}
    assert groups["a"].state is State.STABLE
    assert groups["b"].state is State.EMPTY and groups["b"].is_simple is True
    assert groups["c"].state is State.PREPARING_REBALANCE
    assert groups["d"].state is State.COMPLETING_REBALANCE
    assert groups["e"].state is State.UNKNOWN
    assert groups["f"].state is State.UNKNOWN
    assert admin.timeouts == [6]


def test_listing_that_failed_everywhere_raises() -> None:
    admin = FakeAdminClient(
        listing=ListConsumerGroupsResult(
            valid=[], errors=[_kafka_error(KafkaError.UNKNOWN)]
        )
    )
    with pytest.raises(KafkaMetadataError, match="1 broker"):
        _adapter(admin).list_consumer_groups()


def test_partial_listing_returns_groups_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    admin = FakeAdminClient(
        listing=ListConsumerGroupsResult(
            valid=[ConsumerGroupListing("a", False, ConsumerGroupState.STABLE)],
            errors=[
                _kafka_error(KafkaError._TRANSPORT),
                _kafka_error(KafkaError._TRANSPORT),
                RuntimeError("unexpected"),
            ],
        )
    )
    listing = _adapter(admin).list_consumer_groups()
    assert [g.group_id for g in listing.groups] == ["a"]
    assert listing.complete is False
    assert listing.failed_brokers == 3
    assert listing.failure_codes == ("RuntimeError", "_TRANSPORT")
    assert "consumer group listing is partial" in caplog.text


def test_describe_group_maps_members_and_assignments() -> None:
    admin = FakeAdminClient(
        descriptions={
            "billing": _description(
                "billing",
                ConsumerGroupState.STABLE,
                [_member("m-2", ("pay", 1)), _member("m-1", ("pay", 2), ("pay", 0))],
            )
        }
    )
    group = _adapter(admin).describe_consumer_group("billing")
    assert group.state is State.STABLE
    assert group.partition_assignor == "range"
    assert group.coordinator_id == 1
    assert [m.member_id for m in group.members] == ["m-1", "m-2"]
    assert group.members[0].client_id == "client-m-1"
    assert group.members[0].assignment == (
        TopicPartitionRef("pay", 0),
        TopicPartitionRef("pay", 2),
    )


def test_describe_dead_group_without_members_is_not_found() -> None:
    # Kafka answers DescribeGroups for unknown ids with an empty DEAD group.
    with pytest.raises(KafkaResourceNotFoundError, match="Consumer group 'ghost'"):
        _adapter(FakeAdminClient()).describe_consumer_group("ghost")


def test_describe_group_id_not_found_error_is_not_found() -> None:
    admin = FakeAdminClient(
        descriptions={"ghost": _kafka_error(KafkaError.GROUP_ID_NOT_FOUND)}
    )
    with pytest.raises(KafkaResourceNotFoundError):
        _adapter(admin).describe_consumer_group("ghost")


def test_lag_covers_committed_and_assigned_partitions() -> None:
    admin = FakeAdminClient(
        descriptions={
            "billing": _description(
                "billing",
                ConsumerGroupState.STABLE,
                [_member("m-1", ("pay", 0), ("pay", 2))],
            )
        },
        committed={
            "billing": ConsumerGroupTopicPartitions(
                "billing",
                [
                    TopicPartition("pay", 0, 40),
                    TopicPartition("pay", 1, 10),  # committed, no longer assigned
                    # pay-2 is assigned but has never been committed
                ],
            )
        },
        end_offsets={("pay", 0): _end(100), ("pay", 1): _end(10), ("pay", 2): _end(55)},
    )
    lag = {p.partition: p for p in _adapter(admin).get_consumer_lag("billing")}
    assert admin.offset_requests == [[("pay", 0), ("pay", 1), ("pay", 2)]]
    assert (lag[0].committed_offset, lag[0].log_end_offset, lag[0].lag) == (40, 100, 60)
    assert lag[1].lag == 0
    assert (lag[2].committed_offset, lag[2].log_end_offset, lag[2].lag) == (
        None,
        55,
        None,
    )


def test_invalid_committed_offsets_and_unreadable_end_offsets_are_unknown() -> None:
    admin = FakeAdminClient(
        descriptions={"g": _description("g", ConsumerGroupState.EMPTY, [])},
        committed={
            "g": ConsumerGroupTopicPartitions(
                "g",
                [
                    TopicPartition("t", 0, -1001),  # OFFSET_INVALID
                    TopicPartition("t", 1, 5),  # end offset lookup fails
                ],
            )
        },
        end_offsets={("t", 0): _end(30)},  # ("t", 1) -> _UNKNOWN_PARTITION
    )
    # An EMPTY group with committed offsets exists even without members.
    admin.descriptions["g"] = _description("g", ConsumerGroupState.EMPTY)
    lag = {p.partition: p for p in _adapter(admin).get_consumer_lag("g")}
    assert lag[0].committed_offset is None and lag[0].lag is None
    assert lag[1].log_end_offset is None and lag[1].lag is None


def test_lag_of_group_without_offsets_or_members_is_empty() -> None:
    admin = FakeAdminClient(
        descriptions={"g": _description("g", ConsumerGroupState.EMPTY)}
    )
    assert _adapter(admin).get_consumer_lag("g") == []
    assert admin.offset_requests == []  # nothing to look up


def test_lag_of_missing_group_is_not_found() -> None:
    with pytest.raises(KafkaResourceNotFoundError):
        _adapter(FakeAdminClient()).get_consumer_lag("ghost")


def test_connection_failure_during_end_offset_lookup_fails_the_request() -> None:
    admin = FakeAdminClient(
        descriptions={"g": _description("g", ConsumerGroupState.EMPTY)},
        committed={"g": ConsumerGroupTopicPartitions("g", [TopicPartition("t", 0, 5)])},
        end_offsets={("t", 0): _kafka_error(KafkaError._TRANSPORT)},
    )
    with pytest.raises(KafkaConnectionError):
        _adapter(admin).get_consumer_lag("g")
