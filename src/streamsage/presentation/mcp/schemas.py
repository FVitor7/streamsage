"""Response models: the public, structured output contract of the MCP layer.

These are boundary models (Pydantic) mapped from pure domain entities, so the
wire format can evolve independently of the domain.
"""

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from streamsage.domain.diagnostics.consumer_health import ConsumerHealthReport
from streamsage.domain.diagnostics.consumer_progress import (
    ConsumerProgressReport,
    PartitionProgress,
    ProgressStatus,
)
from streamsage.domain.diagnostics.finding import EvidenceValue, Finding
from streamsage.domain.diagnostics.severity import HealthStatus, Severity
from streamsage.domain.entities.cluster import Broker, Cluster
from streamsage.domain.entities.consumer_group import (
    ConsumerGroup,
    ConsumerGroupListing,
    ConsumerGroupState,
    ConsumerGroupSummary,
    GroupMember,
)
from streamsage.domain.entities.message import TimestampType
from streamsage.domain.entities.topic import Partition, Topic
from streamsage.domain.messages.sanitizer import (
    ContentEncoding,
    InspectedHeader,
    InspectedMessage,
    PayloadTreatment,
)
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
    complete: bool = Field(
        description=(
            "False when some brokers did not answer: groups listed are "
            "accurate, but a group missing from the list may still exist."
        )
    )
    failed_broker_count: int
    failure_codes: list[str] = Field(description="Kafka error codes of the failures.")
    notes: list[str]
    groups: list[ConsumerGroupSummaryResponse]

    @classmethod
    def from_domain(cls, listing: ConsumerGroupListing) -> "ConsumerGroupListResponse":
        notes = []
        if not listing.complete:
            notes.append(
                f"The listing is incomplete: {listing.failed_brokers} broker(s) "
                "did not answer. A group missing from this list may still exist; "
                "use describe_consumer_group to check a specific group."
            )
        return cls(
            group_count=len(listing.groups),
            complete=listing.complete,
            failed_broker_count=listing.failed_brokers,
            failure_codes=list(listing.failure_codes),
            notes=notes,
            groups=[
                ConsumerGroupSummaryResponse.from_domain(g) for g in listing.groups
            ],
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
    summary: str = Field(description="What was observed.")
    evidence: list[EvidenceResponse]
    hypotheses: list[str] = Field(
        description="Possible explanations. Unverified; none is a root cause."
    )
    recommendations: list[str] = Field(description="Suggested read-only next steps.")

    @classmethod
    def from_domain(cls, finding: Finding) -> "FindingResponse":
        return cls(
            code=finding.code,
            severity=finding.severity,
            summary=finding.summary,
            evidence=[
                EvidenceResponse(label=e.label, value=e.value) for e in finding.evidence
            ],
            hypotheses=list(finding.hypotheses),
            recommendations=list(finding.recommendations),
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


class PartitionProgressResponse(BaseModel):
    topic: str
    partition: int
    status: ProgressStatus
    committed_before: int | None
    committed_after: int | None
    committed_delta: int | None = Field(
        description="Messages committed during the window; null if unknown."
    )
    new_messages: int | None = Field(
        description="Log end offset growth during the window; null if unknown."
    )
    lag_after: int | None

    @classmethod
    def from_domain(cls, p: PartitionProgress) -> "PartitionProgressResponse":
        return cls(
            topic=p.topic,
            partition=p.partition,
            status=p.status,
            committed_before=p.committed_before,
            committed_after=p.committed_after,
            committed_delta=p.committed_delta,
            new_messages=p.end_delta,
            lag_after=p.lag_after,
        )


class ConsumerProgressResponse(BaseModel):
    group_id: str
    status: HealthStatus = Field(
        description="Worst finding severity; 'unknown' without usable samples."
    )
    state: ConsumerGroupState
    member_count: int
    interval_seconds: float
    partition_counts: dict[ProgressStatus, int]
    truncated: bool = Field(
        description="True when only the first max_partitions partitions are listed."
    )
    partitions: list[PartitionProgressResponse] = Field(
        description="Not-advancing partitions first, then by lag."
    )
    findings: list[FindingResponse]
    limitations: list[str]

    @classmethod
    def from_domain(
        cls, report: ConsumerProgressReport, *, max_partitions: int
    ) -> "ConsumerProgressResponse":
        listed = report.partitions[:max_partitions]
        return cls(
            group_id=report.group.group_id,
            status=report.status,
            state=report.group.state,
            member_count=report.group.member_count,
            interval_seconds=report.interval_seconds,
            partition_counts={s: report.count(s) for s in ProgressStatus},
            truncated=len(listed) < len(report.partitions),
            partitions=[PartitionProgressResponse.from_domain(p) for p in listed],
            findings=[FindingResponse.from_domain(f) for f in report.findings],
            limitations=list(report.limitations),
        )


class MessageHeaderResponse(BaseModel):
    key: str
    value: str | None = Field(
        description="Decoded UTF-8 value (truncated), '[REDACTED]', or null."
    )
    value_size: int | None
    encoding: ContentEncoding
    redacted: bool
    truncated: bool

    @classmethod
    def from_domain(cls, header: InspectedHeader) -> "MessageHeaderResponse":
        return cls(
            key=header.key,
            value=header.value,
            value_size=header.value_size,
            encoding=header.encoding,
            redacted=header.redacted,
            truncated=header.truncated,
        )


class MessageResponse(BaseModel):
    partition: int
    offset: int
    timestamp: str | None = Field(description="ISO 8601 UTC, or null if unavailable.")
    timestamp_type: TimestampType
    key: str | None = Field(
        description=(
            "Decoded UTF-8 key (truncated), or null if there is no key or it "
            "is withheld."
        )
    )
    key_size: int | None
    key_encoding: ContentEncoding
    key_truncated: bool
    key_withheld: bool = Field(
        description="True if the server is configured not to return keys."
    )
    headers: list[MessageHeaderResponse]
    payload_size: int | None = Field(description="Payload size in bytes; null if none.")
    payload_treatment: PayloadTreatment = Field(
        description=(
            "omitted (payloads disabled), null (no payload), json_redacted "
            "(sensitive fields redacted, then truncated), text_truncated (not "
            "JSON: truncated only), binary (not decoded), too_large (above the "
            "decode limit: not decoded, size only)."
        )
    )
    payload_preview: str | None
    payload_truncated: bool
    redacted_fields: list[str] = Field(
        description="JSON paths whose values were replaced with '[REDACTED]'."
    )

    @classmethod
    def from_domain(cls, message: InspectedMessage) -> "MessageResponse":
        timestamp = (
            None
            if message.timestamp_ms is None
            else datetime.fromtimestamp(message.timestamp_ms / 1000, tz=UTC).isoformat()
        )
        return cls(
            partition=message.partition,
            offset=message.offset,
            timestamp=timestamp,
            timestamp_type=message.timestamp_type,
            key=message.key,
            key_size=message.key_size,
            key_encoding=message.key_encoding,
            key_truncated=message.key_truncated,
            key_withheld=message.key_withheld,
            headers=[MessageHeaderResponse.from_domain(h) for h in message.headers],
            payload_size=message.payload_size,
            payload_treatment=message.payload_treatment,
            payload_preview=message.payload_preview,
            payload_truncated=message.payload_truncated,
            redacted_fields=list(message.redacted_fields),
        )


class MessageInspectionResponse(BaseModel):
    topic: str
    payloads_included: bool = Field(
        description=(
            "False unless the operator enabled payloads; sizes are always shown."
        )
    )
    max_payload_bytes: int
    message_count: int
    complete: bool = Field(description="False if the read stopped early (timeout).")
    exact: bool = Field(
        description=(
            "True when these are guaranteed to be the newest messages (or the "
            "requested range). False when the read budget, unavailable "
            "partitions or a timeout limited what was read; see notes."
        )
    )
    messages_read: int = Field(
        description="Messages read from Kafka to produce this result."
    )
    notes: list[str]
    messages: list[MessageResponse]


class ScanCoverageResponse(BaseModel):
    partition: int
    from_offset: int
    to_offset: int = Field(description="Exclusive.")
    scanned_count: int
    reaches_start: bool = Field(
        description="True if every message currently in the partition was scanned."
    )


class KeySearchResponse(BaseModel):
    topic: str
    match_count: int
    scanned: int = Field(description="Messages read, matching or not.")
    searched_whole_topic: bool = Field(
        description=(
            "True only if every message in the searched partitions was scanned. "
            "If false, no match does not mean the key is absent."
        )
    )
    per_partition_limit: int
    complete: bool
    payloads_included: bool
    coverage: list[ScanCoverageResponse]
    uncovered_partitions: list[int] = Field(
        description=(
            "Partitions not searched at all because the scan budget is "
            "smaller than the number of partitions."
        )
    )
    unavailable_partitions: list[int] = Field(
        description="Partitions whose offsets could not be read."
    )
    notes: list[str]
    matches: list[MessageResponse] = Field(description="Newest first.")


class DlqTopicResponse(BaseModel):
    name: str
    partition_count: int
    message_count: int = Field(
        description="Upper bound from offsets; compaction can make it smaller."
    )


class DlqOverviewResponse(BaseModel):
    configured: bool
    patterns: list[str]
    topics: list[DlqTopicResponse] = Field(description="Most messages first.")
    notes: list[str]
