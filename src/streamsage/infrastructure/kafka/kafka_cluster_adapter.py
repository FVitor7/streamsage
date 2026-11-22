from confluent_kafka.admin._metadata import ClusterMetadata

from streamsage.domain.entities.cluster import Broker, Cluster
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway


class KafkaClusterAdapter:
    """``ClusterReader`` backed by Kafka cluster metadata."""

    def __init__(self, gateway: KafkaAdminGateway) -> None:
        self._gateway = gateway

    def get_cluster_info(self) -> Cluster:
        metadata = self._gateway.cluster_metadata("cluster metadata request")
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
