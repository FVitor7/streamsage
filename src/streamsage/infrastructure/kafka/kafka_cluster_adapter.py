from confluent_kafka.admin._metadata import ClusterMetadata

from streamsage.domain.entities.cluster import Broker, Cluster
from streamsage.infrastructure.kafka.metadata import KafkaMetadataSource


class KafkaClusterAdapter:
    """``ClusterReader`` backed by Kafka cluster metadata."""

    def __init__(self, metadata_source: KafkaMetadataSource) -> None:
        self._metadata_source = metadata_source

    def get_cluster_info(self) -> Cluster:
        metadata = self._metadata_source.fetch("cluster metadata request")
        return to_cluster(metadata)


def to_cluster(metadata: ClusterMetadata) -> Cluster:
    brokers = tuple(
        Broker(broker_id=broker.id, host=str(broker.host), port=broker.port)
        for broker in sorted(metadata.brokers.values(), key=lambda b: b.id)
    )
    controller_id = metadata.controller_id
    return Cluster(
        cluster_id=metadata.cluster_id or None,
        brokers=brokers,
        controller_id=controller_id if controller_id >= 0 else None,
    )
