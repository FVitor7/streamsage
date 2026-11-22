"""Response models: the public, structured output contract of the MCP layer.

These are boundary models (Pydantic) mapped from pure domain entities, so the
wire format can evolve independently of the domain.
"""

from pydantic import BaseModel, Field

from streamsage.domain.diagnostics.consumer_health import ConsumerHealthReport
from streamsage.domain.diagnostics.finding import EvidenceValue, Finding
from streamsage.domain.diagnostics.severity import HealthStatus, Severity
from streamsage.domain.entities.cluster import Broker, Cluster
from streamsage.domain.entities.consumer_group import (
    ConsumerGroup,
    ConsumerGroupState,
    ConsumerGroupSummary,
    GroupMember,
)
from streamsage.domain.entities.topic import Partition, Topic
from streamsage.domain.value_objects.lag import ConsumerGroupLag, PartitionLag


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


class ConsumerGroupSummaryResponse(BaseModel):
    group_id: str
    state: ConsumerGroupState
    is_simple: bool = Field(
        description=(
            "True for groups that only store committed offsets and have no "
            "group protocol (for example offsets set by tooling)."
        )
    )

    @classmethod
    def from_domain(cls, group: ConsumerGroupSummary) -> "ConsumerGroupSummaryResponse":
        return cls(
            group_id=group.group_id, state=group.state, is_simple=group.is_simple
        )


class ConsumerGroupListResponse(BaseModel):
    group_count: int
    groups: list[ConsumerGroupSummaryResponse]

    @classmethod
    def from_domain(
        cls, groups: list[ConsumerGroupSummary]
    ) -> "ConsumerGroupListResponse":
        return cls(
            group_count=len(groups),
            groups=[ConsumerGroupSummaryResponse.from_domain(g) for g in groups],
        )


class TopicPartitionResponse(BaseModel):
    topic: str
    partition: int


class GroupMemberResponse(BaseModel):
    member_id: str
    client_id: str
    host: str
    group_instance_id: str | None
    assignment: list[TopicPartitionResponse]

    @classmethod
    def from_domain(cls, member: GroupMember) -> "GroupMemberResponse":
        return cls(
            member_id=member.member_id,
            client_id=member.client_id,
            host=member.host,
            group_instance_id=member.group_instance_id,
            assignment=[
                TopicPartitionResponse(topic=tp.topic, partition=tp.partition)
                for tp in member.assignment
            ],
        )


class ConsumerGroupDetailResponse(BaseModel):
    group_id: str
    state: ConsumerGroupState
    is_simple: bool
    partition_assignor: str | None
    coordinator_id: int | None = Field(
        description="Broker id of the group coordinator, or null if unknown."
    )
    member_count: int
    members: list[GroupMemberResponse]

    @classmethod
    def from_domain(cls, group: ConsumerGroup) -> "ConsumerGroupDetailResponse":
        return cls(
            group_id=group.group_id,
            state=group.state,
            is_simple=group.is_simple,
            partition_assignor=group.partition_assignor,
            coordinator_id=group.coordinator_id,
            member_count=group.member_count,
            members=[GroupMemberResponse.from_domain(m) for m in group.members],
        )


class PartitionLagResponse(BaseModel):
    topic: str
    partition: int
    committed_offset: int | None = Field(
        description="Committed offset, or null if the group never committed one."
    )
    log_end_offset: int | None = Field(
        description="High watermark when read, or null if it could not be read."
    )
    lag: int | None = Field(
        description=(
            "log_end_offset - committed_offset, or null when either is unknown. "
            "Unknown lag is never reported as 0."
        )
    )

    @classmethod
    def from_domain(cls, partition: PartitionLag) -> "PartitionLagResponse":
        return cls(
            topic=partition.topic,
            partition=partition.partition,
            committed_offset=partition.committed_offset,
            log_end_offset=partition.log_end_offset,
            lag=partition.lag,
        )


class ConsumerLagResponse(BaseModel):
    group_id: str
    total_lag: int | None = Field(
        description="Sum of known partition lags; null if no lag is known."
    )
    max_lag: int | None
    partition_count: int
    unknown_lag_partition_count: int = Field(
        description="Partitions whose lag is unknown and excluded from total_lag."
    )
    truncated: bool = Field(
        description="True when only the max_partitions largest lags are listed."
    )
    partitions: list[PartitionLagResponse] = Field(
        description="Ordered by lag, largest first; unknown lag last."
    )

    @classmethod
    def from_domain(
        cls, lag: ConsumerGroupLag, *, max_partitions: int
    ) -> "ConsumerLagResponse":
        listed = lag.partitions[:max_partitions]
        return cls(
            group_id=lag.group_id,
            total_lag=lag.total_lag,
            max_lag=lag.max_lag,
            partition_count=len(lag.partitions),
            unknown_lag_partition_count=lag.unknown_lag_partition_count,
            truncated=len(listed) < len(lag.partitions),
            partitions=[PartitionLagResponse.from_domain(p) for p in listed],
        )


class EvidenceResponse(BaseModel):
    label: str
    value: EvidenceValue


class FindingResponse(BaseModel):
    code: str = Field(description="Stable identifier of the finding type.")
    severity: Severity
    summary: str
    evidence: list[EvidenceResponse]

    @classmethod
    def from_domain(cls, finding: Finding) -> "FindingResponse":
        return cls(
            code=finding.code,
            severity=finding.severity,
            summary=finding.summary,
            evidence=[
                EvidenceResponse(label=e.label, value=e.value) for e in finding.evidence
            ],
        )


class LagThresholdsResponse(BaseModel):
    warning: int
    critical: int


class ConsumerHealthResponse(BaseModel):
    group_id: str
    status: HealthStatus = Field(
        description=(
            "Worst finding severity; 'unknown' when no lag could be determined "
            "and nothing else was wrong. 'unknown' is not 'healthy'."
        )
    )
    state: ConsumerGroupState
    member_count: int
    total_lag: int | None
    max_lag: int | None
    thresholds: LagThresholdsResponse
    findings: list[FindingResponse]
    limitations: list[str] = Field(
        description="What this assessment cannot show; always relevant."
    )

    @classmethod
    def from_domain(cls, report: ConsumerHealthReport) -> "ConsumerHealthResponse":
        return cls(
            group_id=report.group.group_id,
            status=report.status,
            state=report.group.state,
            member_count=report.group.member_count,
            total_lag=report.lag.total_lag,
            max_lag=report.lag.max_lag,
            thresholds=LagThresholdsResponse(
                warning=report.thresholds.warning,
                critical=report.thresholds.critical,
            ),
            findings=[FindingResponse.from_domain(f) for f in report.findings],
            limitations=list(report.limitations),
        )
