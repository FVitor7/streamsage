"""Evidence-based health classification of a consumer group.

The assessment is deterministic and uses a single snapshot of the group and
its lag. It never claims a consumer is stuck: proving that requires offsets
observed at more than one point in time (see ``consumer_progress``).
"""

from dataclasses import dataclass

from streamsage.domain.diagnostics.evidence import partition_evidence
from streamsage.domain.diagnostics.finding import (
    Evidence,
    Finding,
    SeverityPolicy,
    worst_status,
)
from streamsage.domain.diagnostics.severity import HealthStatus, Severity
from streamsage.domain.entities.consumer_group import (
    ConsumerGroup,
    ConsumerGroupState,
)
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.value_objects.lag import ConsumerGroupLag

LAG_ABOVE_CRITICAL = "LAG_ABOVE_CRITICAL"
LAG_ABOVE_WARNING = "LAG_ABOVE_WARNING"
NO_ACTIVE_MEMBERS = "NO_ACTIVE_MEMBERS"
INACTIVE_CAUGHT_UP = "INACTIVE_CAUGHT_UP"
REBALANCING = "REBALANCING"
UNASSIGNED_PARTITIONS_WITH_LAG = "UNASSIGNED_PARTITIONS_WITH_LAG"
NO_PARTITIONS = "NO_PARTITIONS"
LAG_UNKNOWN = "LAG_UNKNOWN"
STATE_UNKNOWN = "STATE_UNKNOWN"

HEALTH_FINDING_CODES = frozenset(
    {
        LAG_ABOVE_CRITICAL,
        LAG_ABOVE_WARNING,
        NO_ACTIVE_MEMBERS,
        INACTIVE_CAUGHT_UP,
        REBALANCING,
        UNASSIGNED_PARTITIONS_WITH_LAG,
        NO_PARTITIONS,
        LAG_UNKNOWN,
        STATE_UNKNOWN,
    }
)

_REBALANCING = frozenset(
    {ConsumerGroupState.PREPARING_REBALANCE, ConsumerGroupState.COMPLETING_REBALANCE}
)

SNAPSHOT_LIMITATION = (
    "Based on a single snapshot: it shows how far behind the group is, not "
    "whether its offsets are advancing. A large lag can belong to a healthy "
    "consumer catching up. Use detect_stalled_partitions to compare offsets "
    "over time."
)

_CHECK_PROGRESS = (
    "Run detect_stalled_partitions to check whether committed offsets are advancing."
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
    group: ConsumerGroup,
    lag: ConsumerGroupLag,
    thresholds: LagThresholds,
    policy: SeverityPolicy | None = None,
) -> ConsumerHealthReport:
    findings = (policy or SeverityPolicy()).apply(
        [
            *_lag_threshold_findings(lag, thresholds),
            *_membership_findings(group, lag),
            *_rebalance_findings(group),
            *_unassigned_findings(group, lag),
            *_data_gap_findings(group, lag),
        ]
    )
    return ConsumerHealthReport(
        group=group,
        lag=lag,
        thresholds=thresholds,
        status=worst_status(findings, has_data=lag.total_lag is not None),
        findings=findings,
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
    warning = [p for p in lag.partitions if (p.lag or 0) >= thresholds.warning]
    if critical:
        code, severity, offending, limit = (
            LAG_ABOVE_CRITICAL,
            Severity.CRITICAL,
            critical,
            thresholds.critical,
        )
        level = "critical"
    elif warning:
        code, severity, offending, limit = (
            LAG_ABOVE_WARNING,
            Severity.WARNING,
            warning,
            thresholds.warning,
        )
        level = "warning"
    else:
        return []
    return [
        Finding(
            code=code,
            severity=severity,
            summary=(
                f"{len(offending)} partition(s) have lag at or above the {level} "
                f"threshold of {limit} messages."
            ),
            evidence=(Evidence("max_lag", lag.max_lag), *partition_evidence(offending)),
            hypotheses=(
                "Consumers may be slower than producers.",
                "Consumers may have restarted recently and be catching up.",
            ),
            recommendations=(
                _CHECK_PROGRESS,
                "Compare the consumers' processing rate with the producers' rate.",
            ),
        )
    ]


def _membership_findings(group: ConsumerGroup, lag: ConsumerGroupLag) -> list[Finding]:
    if group.members:
        return []
    total = lag.total_lag
    if total is not None and total > 0:
        return [
            Finding(
                code=NO_ACTIVE_MEMBERS,
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
                hypotheses=(
                    "The consuming application may be stopped or unable to join "
                    "the group.",
                    "The group may be obsolete and no longer used.",
                ),
                recommendations=(
                    "Check whether the consuming application is running and can "
                    "reach the cluster.",
                ),
            )
        ]
    if total == 0:
        return [
            Finding(
                code=INACTIVE_CAUGHT_UP,
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
            code=REBALANCING,
            severity=Severity.WARNING,
            summary=(
                "The group is rebalancing; members pause consumption during a "
                "rebalance. One observation cannot tell whether it is stuck."
            ),
            evidence=(
                Evidence("state", group.state.value),
                Evidence("member_count", group.member_count),
            ),
            hypotheses=(
                "Members may be joining or leaving, for example during a deploy.",
                "Members exceeding max.poll.interval.ms can cause repeated rebalances.",
            ),
            recommendations=(
                "Analyze the group again shortly; a rebalance that persists over "
                "several observations deserves investigation.",
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
            code=UNASSIGNED_PARTITIONS_WITH_LAG,
            severity=Severity.WARNING,
            summary=(
                f"{len(unassigned)} partition(s) with pending messages are not "
                "assigned to any member."
            ),
            evidence=partition_evidence(unassigned),
            hypotheses=("The group may no longer subscribe to these topics.",),
            recommendations=(
                "Confirm the group's topic subscription; if these topics are no "
                "longer consumed, their committed offsets are stale.",
            ),
        )
    ]


def _data_gap_findings(group: ConsumerGroup, lag: ConsumerGroupLag) -> list[Finding]:
    findings: list[Finding] = []
    if not lag.partitions:
        findings.append(
            Finding(
                code=NO_PARTITIONS,
                severity=Severity.INFO,
                summary=(
                    "Insufficient data: the group has no committed offsets and no "
                    "assigned partitions, so its lag cannot be assessed."
                ),
                evidence=(Evidence("state", group.state.value),),
            )
        )
    unknown = [p for p in lag.partitions if p.lag is None]
    if unknown:
        findings.append(
            Finding(
                code=LAG_UNKNOWN,
                severity=Severity.INFO,
                summary=(
                    f"Insufficient data: lag is unknown for {len(unknown)} "
                    "partition(s) (no committed offset, or the end offset could "
                    "not be read)."
                ),
                evidence=partition_evidence(unknown),
                hypotheses=(
                    "A newly assigned partition has no committed offset until the "
                    "consumer commits for the first time.",
                ),
            )
        )
    if group.state is ConsumerGroupState.UNKNOWN:
        findings.append(
            Finding(
                code=STATE_UNKNOWN,
                severity=Severity.INFO,
                summary=(
                    "Insufficient data: the group coordinator did not report a "
                    "known group state."
                ),
            )
        )
    return findings
