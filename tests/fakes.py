"""In-memory test doubles for the application ports."""

import secrets
from collections.abc import Sequence

from streamsage.domain.entities.cluster import Broker, Cluster
from streamsage.domain.entities.consumer_group import (
    ConsumerGroup,
    ConsumerGroupState,
    ConsumerGroupSummary,
    GroupMember,
    TopicPartitionRef,
)
from streamsage.domain.entities.message import (
    KeyScanResult,
    MessageReadResult,
    OffsetRange,
    PartitionOffsets,
    RawHeader,
    RawMessage,
    TimestampType,
)
from streamsage.domain.entities.topic import Partition, Topic
from streamsage.domain.exceptions import KafkaResourceNotFoundError
from streamsage.domain.value_objects.lag import PartitionLag


def make_topic(
    name: str,
    partitions: int = 1,
    *,
    replicas: tuple[int, ...] = (1,),
    isr: tuple[int, ...] | None = None,
    leader: int | None = 1,
    internal: bool | None = None,
) -> Topic:
    return Topic(
        name=name,
        partitions=tuple(
            Partition(
                partition_id=i,
                leader=leader,
                replicas=replicas,
                isr=replicas if isr is None else isr,
            )
            for i in range(partitions)
        ),
        internal=name.startswith("__") if internal is None else internal,
    )


def random_credential() -> str:
    """Generate a throwaway credential per test run.

    Tests never hardcode credential-looking literals, so secret scanners have
    nothing to flag and leak assertions cannot pass by coincidence.
    """
    return secrets.token_urlsafe(24)


SAMPLE_CLUSTER = Cluster(
    cluster_id="test-cluster",
    brokers=(Broker(1, "broker-1", 9092), Broker(2, "broker-2", 9092)),
    controller_id=1,
)


class FakeClusterReader:
    def __init__(
        self, cluster: Cluster = SAMPLE_CLUSTER, error: Exception | None = None
    ) -> None:
        self.cluster = cluster
        self.error = error

    def get_cluster_info(self) -> Cluster:
        if self.error is not None:
            raise self.error
        return self.cluster


class FakeTopicReader:
    def __init__(
        self, topics: list[Topic] | None = None, error: Exception | None = None
    ) -> None:
        self.topics = topics if topics is not None else []
        self.error = error
        self.described: list[str] = []

    def list_topics(self) -> list[Topic]:
        if self.error is not None:
            raise self.error
        return list(self.topics)

    def describe_topic(self, name: str) -> Topic:
        self.described.append(name)
        if self.error is not None:
            raise self.error
        for topic in self.topics:
            if topic.name == name:
                return topic
        raise KafkaResourceNotFoundError(f"Topic '{name}' does not exist.")


SAMPLE_GROUPS = [
    ConsumerGroupSummary("orders-service", ConsumerGroupState.EMPTY, is_simple=True),
    ConsumerGroupSummary("billing", ConsumerGroupState.STABLE, is_simple=False),
    ConsumerGroupSummary("legacy", ConsumerGroupState.DEAD, is_simple=False),
]

SAMPLE_GROUP = ConsumerGroup(
    group_id="billing",
    state=ConsumerGroupState.STABLE,
    is_simple=False,
    partition_assignor="range",
    coordinator_id=1,
    members=(
        GroupMember(
            member_id="m-1",
            client_id="billing-1",
            host="/10.0.0.5",
            group_instance_id=None,
            assignment=(
                TopicPartitionRef("payments", 1),
                TopicPartitionRef("payments", 0),
            ),
        ),
    ),
)

SAMPLE_LAG = [
    PartitionLag("payments", 0, committed_offset=40, log_end_offset=100),
    PartitionLag("payments", 1, committed_offset=None, log_end_offset=80),
    PartitionLag("payments", 2, committed_offset=90, log_end_offset=100),
]


class FakeConsumerGroupReader:
    def __init__(
        self,
        groups: list[ConsumerGroupSummary] | None = None,
        group: ConsumerGroup = SAMPLE_GROUP,
        lag: list[PartitionLag] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.groups = SAMPLE_GROUPS if groups is None else groups
        self.group = group
        self.lag = SAMPLE_LAG if lag is None else lag
        self.error = error
        self.requested: list[str] = []

    def list_consumer_groups(self) -> list[ConsumerGroupSummary]:
        if self.error is not None:
            raise self.error
        return list(self.groups)

    def describe_consumer_group(self, group_id: str) -> ConsumerGroup:
        self.requested.append(group_id)
        if self.error is not None:
            raise self.error
        if group_id != self.group.group_id:
            raise KafkaResourceNotFoundError(
                f"Consumer group '{group_id}' does not exist."
            )
        return self.group

    def get_consumer_lag(self, group_id: str) -> list[PartitionLag]:
        self.describe_consumer_group(group_id)
        return list(self.lag)


def raw_message(
    partition: int,
    offset: int,
    value: bytes | None = b'{"ok":true}',
    timestamp_ms: int | None = None,
    topic: str = "orders",
) -> RawMessage:
    return RawMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp_ms=1_700_000_000_000 + offset
        if timestamp_ms is None
        else timestamp_ms,
        timestamp_type=TimestampType.CREATE_TIME,
        key=f"k-{offset}".encode(),
        value=value,
        headers=(RawHeader("trace-id", b"t"),),
    )


class FakeMessageReader:
    """Serves messages from an in-memory log, honoring the requested ranges."""

    def __init__(
        self,
        offsets: list[PartitionOffsets] | None = None,
        messages: list[RawMessage] | None = None,
        complete: bool = True,
        error: Exception | None = None,
    ) -> None:
        self.offsets = (
            [PartitionOffsets(0, 0, 10), PartitionOffsets(1, 0, 4)]
            if offsets is None
            else offsets
        )
        self.messages = (
            [raw_message(0, o) for o in range(10)]
            + [raw_message(1, o) for o in range(4)]
            if messages is None
            else messages
        )
        self.complete = complete
        self.error = error
        self.requested_ranges: list[tuple[OffsetRange, ...]] = []

    def partition_offsets(self, topic: str) -> list[PartitionOffsets]:
        if self.error is not None:
            raise self.error
        if topic == "missing":
            raise KafkaResourceNotFoundError(f"Topic '{topic}' does not exist.")
        return list(self.offsets)

    def read_range(
        self, topic: str, ranges: Sequence[OffsetRange]
    ) -> MessageReadResult:
        self.requested_ranges.append(tuple(ranges))
        selected = tuple(
            m
            for m in self.messages
            for r in ranges
            if m.partition == r.partition and r.start <= m.offset < r.stop
        )
        return MessageReadResult(messages=selected, complete=self.complete)

    def find_by_key(
        self,
        topic: str,
        ranges: Sequence[OffsetRange],
        key: bytes,
        max_matches_per_partition: int,
    ) -> KeyScanResult:
        self.requested_ranges.append(tuple(ranges))
        scanned = [
            m
            for m in self.messages
            for r in ranges
            if m.partition == r.partition and r.start <= m.offset < r.stop
        ]
        matches: list[RawMessage] = []
        for partition in {r.partition for r in ranges}:
            found = [m for m in scanned if m.partition == partition and m.key == key]
            matches.extend(found[-max_matches_per_partition:])
        return KeyScanResult(
            matches=tuple(matches), scanned=len(scanned), complete=self.complete
        )
