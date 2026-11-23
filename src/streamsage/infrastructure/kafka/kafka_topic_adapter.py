from confluent_kafka import KafkaError, Node, TopicCollection, TopicPartitionInfo
from confluent_kafka.admin import PartitionMetadata, TopicDescription, TopicMetadata

from streamsage.domain.entities.topic import Partition, Topic
from streamsage.infrastructure.kafka.errors import error_code_name
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway

# Cluster metadata (used for listing) does not carry an "internal" flag.
# Kafka reserves the "__" prefix for internal topics such as
# __consumer_offsets, so listings use that convention. `describe_topic`
# uses the flag reported by Kafka itself.
_INTERNAL_TOPIC_PREFIX = "__"


class KafkaTopicAdapter:
    """``TopicReader`` backed by the Kafka AdminClient. Never creates topics."""

    def __init__(self, gateway: KafkaAdminGateway) -> None:
        self._gateway = gateway

    def list_topics(self) -> list[Topic]:
        metadata = self._gateway.cluster_metadata("topic listing")
        return [
            from_metadata(name, topic)
            for name, topic in metadata.topics.items()
            if not _is_unknown_topic(topic.error)
        ]

    def describe_topic(self, name: str) -> Topic:
        # DescribeTopics requests only this topic and does not trigger topic
        # auto-creation, unlike a topic-scoped Metadata request. This is
        # guarded by test_missing_topic_raises_and_is_never_created.
        description: TopicDescription = self._gateway.request(
            "topic description",
            lambda admin, timeout: admin.describe_topics(
                TopicCollection([name]), request_timeout=timeout
            )[name],
            not_found_message=f"Topic '{name}' does not exist.",
        )
        return from_description(description)


def _is_unknown_topic(error: KafkaError | None) -> bool:
    return error is not None and error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART


def from_metadata(name: str, metadata: TopicMetadata) -> Topic:
    partitions = tuple(
        Partition(
            partition_id=p.id,
            leader=p.leader if p.leader >= 0 else None,
            replicas=tuple(p.replicas),
            isr=tuple(p.isrs),
            metadata_error=error_code_name(p.error),
        )
        for p in sorted(metadata.partitions.values(), key=_partition_id)
    )
    return Topic(
        name=name,
        partitions=partitions,
        internal=name.startswith(_INTERNAL_TOPIC_PREFIX),
        metadata_error=error_code_name(metadata.error),
    )


def from_description(description: TopicDescription) -> Topic:
    partitions = tuple(
        Partition(
            partition_id=p.id,
            leader=_node_id(p.leader),
            replicas=tuple(n.id for n in p.replicas),
            isr=tuple(n.id for n in p.isr),
        )
        for p in sorted(description.partitions, key=_partition_info_id)
    )
    return Topic(
        name=description.name,
        partitions=partitions,
        internal=bool(description.is_internal),
    )


def _node_id(node: Node | None) -> int | None:
    if node is None or node.id < 0:
        return None
    return node.id


def _partition_id(partition: PartitionMetadata) -> int:
    return partition.id


def _partition_info_id(partition: TopicPartitionInfo) -> int:
    return partition.id
