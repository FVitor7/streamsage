import logging
from enum import Enum

from confluent_kafka import (
    ConsumerGroupTopicPartitions,
    KafkaError,
    KafkaException,
    Node,
    TopicPartition,
)
from confluent_kafka.admin._group import (
    ConsumerGroupDescription,
    ListConsumerGroupsResult,
    MemberDescription,
)
from confluent_kafka.admin._group import (
    ConsumerGroupListing as KafkaGroupListing,
)
from confluent_kafka.admin._listoffsets import ListOffsetsResultInfo, OffsetSpec

from streamsage.domain.entities.consumer_group import (
    ConsumerGroup,
    ConsumerGroupListing,
    ConsumerGroupState,
    ConsumerGroupSummary,
    GroupMember,
    TopicPartitionRef,
)
from streamsage.domain.exceptions import (
    KafkaMetadataError,
    KafkaResourceNotFoundError,
)
from streamsage.domain.value_objects.lag import PartitionLag
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway

logger = logging.getLogger(__name__)

# confluent_kafka.ConsumerGroupState member name -> domain state.
_STATES = {
    "STABLE": ConsumerGroupState.STABLE,
    "EMPTY": ConsumerGroupState.EMPTY,
    "PREPARING_REBALANCING": ConsumerGroupState.PREPARING_REBALANCE,
    "COMPLETING_REBALANCING": ConsumerGroupState.COMPLETING_REBALANCE,
    "DEAD": ConsumerGroupState.DEAD,
}


class KafkaConsumerGroupAdapter:
    """``ConsumerGroupReader`` backed by the Kafka AdminClient (read-only)."""

    def __init__(self, gateway: KafkaAdminGateway) -> None:
        self._gateway = gateway

    def list_consumer_groups(self) -> ConsumerGroupListing:
        result: ListConsumerGroupsResult = self._gateway.request(
            "consumer group listing",
            lambda admin, timeout: admin.list_consumer_groups(request_timeout=timeout),
        )
        valid = result.valid or []
        errors = result.errors or []
        if errors and not valid:
            raise KafkaMetadataError(
                f"Consumer group listing failed on {len(errors)} broker(s)."
            )
        codes = tuple(sorted({_error_code(error) for error in errors}))
        if errors:
            # Some brokers answered and others did not: the listing may be
            # incomplete. Groups that were returned are still accurate.
            logger.warning(
                "consumer group listing is partial",
                extra={"event": {"failed_brokers": len(errors), "error_codes": codes}},
            )
        return ConsumerGroupListing(
            groups=tuple(to_summary(listing) for listing in valid),
            failed_brokers=len(errors),
            failure_codes=codes,
        )

    def describe_consumer_group(self, group_id: str) -> ConsumerGroup:
        description: ConsumerGroupDescription = self._gateway.request(
            "consumer group description",
            lambda admin, timeout: admin.describe_consumer_groups(
                [group_id], request_timeout=timeout
            )[group_id],
            not_found_message=_not_found(group_id),
        )
        group = to_group(description)
        # The coordinator answers DescribeGroups for unknown ids with an empty
        # group in state DEAD instead of an error.
        if group.state is ConsumerGroupState.DEAD and not group.members:
            raise KafkaResourceNotFoundError(_not_found(group_id))
        return group

    def get_consumer_lag(self, group_id: str) -> list[PartitionLag]:
        group = self.describe_consumer_group(group_id)
        committed = self._committed_offsets(group_id)
        partitions = sorted(set(committed) | set(group.assigned_partitions))
        end_offsets = self._log_end_offsets(partitions)
        return [
            PartitionLag(
                topic=tp.topic,
                partition=tp.partition,
                committed_offset=committed.get(tp),
                log_end_offset=end_offsets.get(tp),
            )
            for tp in partitions
        ]

    def _committed_offsets(self, group_id: str) -> dict[TopicPartitionRef, int | None]:
        result: ConsumerGroupTopicPartitions = self._gateway.request(
            "committed offset lookup",
            lambda admin, timeout: admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group_id)], request_timeout=timeout
            )[group_id],
            not_found_message=_not_found(group_id),
        )
        return {
            TopicPartitionRef(tp.topic, tp.partition): (
                # Negative offsets (OFFSET_INVALID) mean "nothing committed".
                tp.offset if tp.error is None and tp.offset >= 0 else None
            )
            for tp in result.topic_partitions or []
        }

    def _log_end_offsets(
        self, partitions: list[TopicPartitionRef]
    ) -> dict[TopicPartitionRef, int | None]:
        if not partitions:
            return {}
        requested = {
            # OffsetSpec.latest() lacks type annotations upstream.
            TopicPartition(tp.topic, tp.partition): OffsetSpec.latest()  # type: ignore[no-untyped-call]
            for tp in partitions
        }
        results: dict[TopicPartition, ListOffsetsResultInfo | None] = (
            self._gateway.request_each(
                "log end offset lookup",
                lambda admin, timeout: admin.list_offsets(
                    requested, request_timeout=timeout
                ),
            )
        )
        return {
            TopicPartitionRef(tp.topic, tp.partition): (
                info.offset if info is not None and info.offset >= 0 else None
            )
            for tp, info in results.items()
        }


def _error_code(error: object) -> str:
    """Symbolic Kafka error code of a listing failure (never its message)."""
    if isinstance(error, KafkaException) and error.args:
        error = error.args[0]
    if isinstance(error, KafkaError):
        return error.name()
    return type(error).__name__


def _not_found(group_id: str) -> str:
    return f"Consumer group '{group_id}' does not exist."


def to_state(state: Enum | None) -> ConsumerGroupState:
    # confluent-kafka leaves the `state` attribute unset (rather than None)
    # when the broker does not report it; callers read it with getattr.
    if state is None:
        return ConsumerGroupState.UNKNOWN
    return _STATES.get(state.name, ConsumerGroupState.UNKNOWN)


def to_summary(listing: KafkaGroupListing) -> ConsumerGroupSummary:
    return ConsumerGroupSummary(
        group_id=listing.group_id,
        state=to_state(getattr(listing, "state", None)),
        is_simple=bool(listing.is_simple_consumer_group),
    )


def to_group(description: ConsumerGroupDescription) -> ConsumerGroup:
    return ConsumerGroup(
        group_id=description.group_id,
        state=to_state(getattr(description, "state", None)),
        is_simple=bool(description.is_simple_consumer_group),
        partition_assignor=description.partition_assignor or None,
        coordinator_id=_node_id(description.coordinator),
        members=tuple(
            to_member(member)
            for member in sorted(description.members or [], key=_member_id)
        ),
    )


def to_member(member: MemberDescription) -> GroupMember:
    partitions = member.assignment.topic_partitions if member.assignment else []
    return GroupMember(
        member_id=member.member_id,
        client_id=member.client_id,
        host=member.host,
        group_instance_id=member.group_instance_id,
        assignment=tuple(
            sorted(TopicPartitionRef(tp.topic, tp.partition) for tp in partitions)
        ),
    )


def _member_id(member: MemberDescription) -> str:
    return str(member.member_id)


def _node_id(node: Node | None) -> int | None:
    if node is None or node.id < 0:
        return None
    return node.id
