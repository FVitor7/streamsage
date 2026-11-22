"""Response models: the public, structured output contract of the MCP layer.

These are boundary models (Pydantic) mapped from pure domain entities, so the
wire format can evolve independently of the domain.
"""

from pydantic import BaseModel, Field

from streamsage.domain.entities.cluster import Broker, Cluster
from streamsage.domain.entities.topic import Partition, Topic


class BrokerResponse(BaseModel):
    broker_id: int
    host: str
    port: int

    @classmethod
    def from_domain(cls, broker: Broker) -> "BrokerResponse":
        return cls(broker_id=broker.broker_id, host=broker.host, port=broker.port)


class ClusterInfoResponse(BaseModel):
    cluster_id: str | None = Field(
        description="Cluster id, or null if the cluster does not report one."
    )
    controller_id: int | None = Field(
        description=(
            "Controller id as reported in the Metadata response, or null if not "
            "reported. In KRaft mode Kafka reports a live broker id here, not "
            "the active quorum controller."
        )
    )
    broker_count: int
    brokers: list[BrokerResponse]

    @classmethod
    def from_domain(cls, cluster: Cluster) -> "ClusterInfoResponse":
        return cls(
            cluster_id=cluster.cluster_id,
            controller_id=cluster.controller_id,
            broker_count=cluster.broker_count,
            brokers=[BrokerResponse.from_domain(b) for b in cluster.brokers],
        )


class TopicSummaryResponse(BaseModel):
    name: str
    internal: bool
    partition_count: int
    replication_factor: int | None
    metadata_error: str | None

    @classmethod
    def from_domain(cls, topic: Topic) -> "TopicSummaryResponse":
        return cls(
            name=topic.name,
            internal=topic.internal,
            partition_count=topic.partition_count,
            replication_factor=topic.replication_factor,
            metadata_error=topic.metadata_error,
        )


class TopicListResponse(BaseModel):
    include_internal: bool
    topic_count: int
    topics: list[TopicSummaryResponse]

    @classmethod
    def from_domain(
        cls, topics: list[Topic], *, include_internal: bool
    ) -> "TopicListResponse":
        return cls(
            include_internal=include_internal,
            topic_count=len(topics),
            topics=[TopicSummaryResponse.from_domain(t) for t in topics],
        )


class PartitionResponse(BaseModel):
    partition_id: int
    leader: int | None = Field(description="Leader broker id, or null if none.")
    replicas: list[int]
    isr: list[int] = Field(description="In-sync replica broker ids.")
    under_replicated: bool
    metadata_error: str | None

    @classmethod
    def from_domain(cls, partition: Partition) -> "PartitionResponse":
        return cls(
            partition_id=partition.partition_id,
            leader=partition.leader,
            replicas=list(partition.replicas),
            isr=list(partition.isr),
            under_replicated=partition.is_under_replicated,
            metadata_error=partition.metadata_error,
        )


class TopicDetailResponse(BaseModel):
    name: str
    internal: bool
    partition_count: int
    replication_factor: int | None
    under_replicated_partition_count: int
    leaderless_partition_count: int
    metadata_error: str | None
    partitions: list[PartitionResponse]

    @classmethod
    def from_domain(cls, topic: Topic) -> "TopicDetailResponse":
        return cls(
            name=topic.name,
            internal=topic.internal,
            partition_count=topic.partition_count,
            replication_factor=topic.replication_factor,
            under_replicated_partition_count=len(topic.under_replicated_partitions),
            leaderless_partition_count=len(topic.leaderless_partitions),
            metadata_error=topic.metadata_error,
            partitions=[PartitionResponse.from_domain(p) for p in topic.partitions],
        )
