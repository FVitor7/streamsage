from confluent_kafka import KafkaError
from confluent_kafka.admin._metadata import PartitionMetadata, TopicMetadata

from streamsage.domain.entities.topic import Partition, Topic
from streamsage.domain.exceptions import KafkaResourceNotFoundError
from streamsage.infrastructure.kafka.errors import error_code_name
from streamsage.infrastructure.kafka.metadata import KafkaMetadataSource

# Kafka reserves the "__" prefix for internal topics such as
# __consumer_offsets and __transaction_state. Cluster metadata does not carry
# an "internal" flag, so the naming convention is the source of truth here.
_INTERNAL_TOPIC_PREFIX = "__"


class KafkaTopicAdapter:
    """``TopicReader`` backed by Kafka cluster metadata. Never creates topics."""

    def __init__(self, metadata_source: KafkaMetadataSource) -> None:
        self._metadata_source = metadata_source

    def list_topics(self) -> list[Topic]:
        metadata = self._metadata_source.fetch("topic listing")
        return [to_topic(name, topic) for name, topic in metadata.topics.items()]

    def describe_topic(self, name: str) -> Topic:
        # Look the topic up in the full listing instead of requesting metadata
        # for `name` directly, so a missing topic can never be auto-created.
        metadata = self._metadata_source.fetch("topic description")
        topic = metadata.topics.get(name)
        if topic is None or _is_unknown_topic(topic.error):
            raise KafkaResourceNotFoundError(f"Topic '{name}' does not exist.")
        return to_topic(name, topic)


def _is_unknown_topic(error: KafkaError | None) -> bool:
    return error is not None and error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART


def to_topic(name: str, metadata: TopicMetadata) -> Topic:
    partitions = tuple(
        to_partition(partition)
        for partition in sorted(metadata.partitions.values(), key=lambda p: p.id)
    )
    return Topic(
        name=name,
        partitions=partitions,
        internal=name.startswith(_INTERNAL_TOPIC_PREFIX),
        metadata_error=error_code_name(metadata.error),
    )


def to_partition(metadata: PartitionMetadata) -> Partition:
    return Partition(
        partition_id=metadata.id,
        leader=metadata.leader if metadata.leader >= 0 else None,
        replicas=tuple(metadata.replicas),
        isr=tuple(metadata.isrs),
        metadata_error=error_code_name(metadata.error),
    )
