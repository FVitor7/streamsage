"""Evidence-based health classification of a consumer group.

The assessment is deterministic and uses a single snapshot of the group and
its lag. It never claims a consumer is stuck: proving that requires offsets
observed at more than one point in time.
"""

from dataclasses import dataclass

from streamsage.domain.diagnostics.finding import Evidence, Finding
from streamsage.domain.diagnostics.severity import HealthStatus, Severity
from streamsage.domain.entities.consumer_group import (
    ConsumerGroup,
    ConsumerGroupState,
)
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.value_objects.lag import ConsumerGroupLag, PartitionLag

# Findings list at most this many partitions as evidence.
MAX_EVIDENCE_PARTITIONS = 5

_REBALANCING = frozenset(
    {ConsumerGroupState.PREPARING_REBALANCE, ConsumerGroupState.COMPLETING_REBALANCE}
)

SNAPSHOT_LIMITATION = (
    "Based on a single snapshot: it shows how far behind the group is, not "
    "whether its offsets are advancing. A large lag can belong to a healthy "
    "consumer catching up."
)


@dataclass(frozen=True, slots=True)
class LagThresholds:
    """Per-partition lag thresholds, in messages."""

    warning: int
    critical: int

    def __post_init__(self) -> None:
        if self.warning < 1:
            raise InvalidInputError("Lag warning threshold must be at least 1.")
        if self.critical < self.warning:
            raise InvalidInputError(
                "Lag critical threshold must be greater than or equal to the "
                "warning threshold."
            )


@dataclass(frozen=True, slots=True)
class ConsumerHealthReport:
    group: ConsumerGroup
    lag: ConsumerGroupLag
    thresholds: LagThresholds
    status: HealthStatus
    findings: tuple[Finding, ...]
    limitations: tuple[str, ...]


def assess_consumer_health(
    group: ConsumerGroup, lag: ConsumerGroupLag, thresholds: LagThresholds
) -> ConsumerHealthReport:
    findings = [
        *_lag_threshold_findings(lag, thresholds),
        *_membership_findings(group, lag),
        *_rebalance_findings(group),
        *_unassigned_findings(group, lag),
        *_data_gap_findings(group, lag),
    ]
    return ConsumerHealthReport(
        group=group,
        lag=lag,
        thresholds=thresholds,
        status=_overall_status(findings, lag),
        findings=tuple(findings),
        limitations=(
            SNAPSHOT_LIMITATION,
            f"Lag thresholds are configured values (warning >= "
            f"{thresholds.warning}, critical >= {thresholds.critical} messages "
            "per partition), not derived from this consumer's throughput.",
        ),
    )


def _lag_threshold_findings(
    lag: ConsumerGroupLag, thresholds: LagThresholds
) -> list[Finding]:
    critical = [p for p in lag.partitions if (p.lag or 0) >= thresholds.critical]
    if critical:
        return [
            Finding(
                code="LAG_ABOVE_CRITICAL",
                severity=Severity.CRITICAL,
                summary=(
                    f"{len(critical)} partition(s) have lag at or above the "
                    f"critical threshold of {thresholds.critical} messages."
                ),
                evidence=(
                    Evidence("max_lag", lag.max_lag),
                    *_partition_evidence(critical),
                ),
            )
        ]
    warning = [p for p in lag.partitions if (p.lag or 0) >= thresholds.warning]
    if warning:
        return [
            Finding(
                code="LAG_ABOVE_WARNING",
                severity=Severity.WARNING,
                summary=(
                    f"{len(warning)} partition(s) have lag at or above the "
                    f"warning threshold of {thresholds.warning} messages."
                ),
                evidence=(
                    Evidence("max_lag", lag.max_lag),
                    *_partition_evidence(warning),
                ),
            )
        ]
    return []


def _membership_findings(group: ConsumerGroup, lag: ConsumerGroupLag) -> list[Finding]:
    if group.members:
        return []
    total = lag.total_lag
    if total is not None and total > 0:
        return [
            Finding(
                code="NO_ACTIVE_MEMBERS",
                severity=Severity.WARNING,
                summary=(
                    f"The group has no active members while {total} message(s) "
                    "are pending, so nothing in this group is consuming them."
                ),
                evidence=(
                    Evidence("state", group.state.value),
                    Evidence("member_count", 0),
                    Evidence("total_lag", total),
                ),
            )
        ]
    if total == 0:
        return [
            Finding(
                code="INACTIVE_CAUGHT_UP",
                severity=Severity.INFO,
                summary="The group has no active members and no pending messages.",
                evidence=(
                    Evidence("state", group.state.value),
                    Evidence("total_lag", 0),
                ),
            )
        ]
    return []


def _rebalance_findings(group: ConsumerGroup) -> list[Finding]:
    if group.state not in _REBALANCING:
        return []
    return [
        Finding(
            code="REBALANCING",
            severity=Severity.WARNING,
            summary=(
                "The group is rebalancing; members pause consumption during a "
                "rebalance. One observation cannot tell whether it is stuck."
            ),
            evidence=(
                Evidence("state", group.state.value),
                Evidence("member_count", group.member_count),
            ),
        )
    ]


def _unassigned_findings(group: ConsumerGroup, lag: ConsumerGroupLag) -> list[Finding]:
    if not group.members:
        return []
    assigned = {(tp.topic, tp.partition) for tp in group.assigned_partitions}
    unassigned = [
        p
        for p in lag.partitions
        if (p.topic, p.partition) not in assigned and (p.lag or 0) > 0
    ]
    if not unassigned:
        return []
    return [
        Finding(
            code="UNASSIGNED_PARTITIONS_WITH_LAG",
            severity=Severity.WARNING,
            summary=(
                f"{len(unassigned)} partition(s) with pending messages are not "
                "assigned to any member. Possible explanation (unverified): the "
                "group no longer subscribes to their topic."
            ),
            evidence=tuple(_partition_evidence(unassigned)),
        )
    ]


def _data_gap_findings(group: ConsumerGroup, lag: ConsumerGroupLag) -> list[Finding]:
    findings: list[Finding] = []
    if not lag.partitions:
        findings.append(
            Finding(
                code="NO_PARTITIONS",
                severity=Severity.INFO,
                summary=(
                    "The group has no committed offsets and no assigned "
                    "partitions, so its lag cannot be assessed."
                ),
                evidence=(Evidence("state", group.state.value),),
            )
        )
    unknown = [p for p in lag.partitions if p.lag is None]
    if unknown:
        findings.append(
            Finding(
                code="LAG_UNKNOWN",
                severity=Severity.INFO,
                summary=(
                    f"Lag is unknown for {len(unknown)} partition(s): no committed "
                    "offset, or the end offset could not be read."
                ),
                evidence=tuple(_partition_evidence(unknown)),
            )
        )
    if group.state is ConsumerGroupState.UNKNOWN:
        findings.append(
            Finding(
                code="STATE_UNKNOWN",
                severity=Severity.INFO,
                summary="The group coordinator did not report a known group state.",
            )
        )
    return findings


def _overall_status(findings: list[Finding], lag: ConsumerGroupLag) -> HealthStatus:
    severities = {finding.severity for finding in findings}
    if Severity.CRITICAL in severities:
        return HealthStatus.CRITICAL
    if Severity.WARNING in severities:
        return HealthStatus.WARNING
    if lag.total_lag is None:
        return HealthStatus.UNKNOWN
    return HealthStatus.HEALTHY


def _partition_evidence(partitions: list[PartitionLag]) -> list[Evidence]:
    evidence = [
        Evidence(f"{p.topic}[{p.partition}].lag", p.lag)
        for p in partitions[:MAX_EVIDENCE_PARTITIONS]
    ]
    if len(partitions) > MAX_EVIDENCE_PARTITIONS:
        evidence.append(
            Evidence("more_partitions", len(partitions) - MAX_EVIDENCE_PARTITIONS)
        )
    return evidence
