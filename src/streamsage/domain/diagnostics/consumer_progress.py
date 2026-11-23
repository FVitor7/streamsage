"""Detection of partitions whose committed offsets are not advancing.

High lag does not mean a consumer is stuck. Absence of progress can only be
observed by comparing offsets at more than one point in time, so this
analysis takes two lag samples of the same group, ``interval_seconds``
apart, and compares them partition by partition.
"""

from dataclasses import dataclass
from enum import StrEnum

from streamsage.domain.diagnostics.evidence import MAX_EVIDENCE_PARTITIONS
from streamsage.domain.diagnostics.finding import (
    Evidence,
    Finding,
    SeverityPolicy,
    worst_status,
)
from streamsage.domain.diagnostics.severity import HealthStatus, Severity
from streamsage.domain.entities.consumer_group import ConsumerGroup
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.value_objects.lag import ConsumerGroupLag, PartitionLag

MIN_SAMPLE_INTERVAL_SECONDS = 1
MAX_SAMPLE_INTERVAL_SECONDS = 60

PARTITIONS_NOT_ADVANCING = "PARTITIONS_NOT_ADVANCING"
COMMITTED_OFFSET_MOVED_BACKWARD = "COMMITTED_OFFSET_MOVED_BACKWARD"
PROGRESS_UNKNOWN = "PROGRESS_UNKNOWN"
NO_PARTITIONS_SAMPLED = "NO_PARTITIONS_SAMPLED"

PROGRESS_FINDING_CODES = frozenset(
    {
        PARTITIONS_NOT_ADVANCING,
        COMMITTED_OFFSET_MOVED_BACKWARD,
        PROGRESS_UNKNOWN,
        NO_PARTITIONS_SAMPLED,
    }
)


class ProgressStatus(StrEnum):
    ADVANCING = "advancing"
    CAUGHT_UP = "caught_up"
    NOT_ADVANCING = "not_advancing"
    MOVED_BACKWARD = "moved_backward"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class PartitionProgress:
    """How one partition's offsets changed between the two samples."""

    topic: str
    partition: int
    committed_before: int | None
    committed_after: int | None
    end_before: int | None
    end_after: int | None
    lag_after: int | None
    status: ProgressStatus

    @property
    def committed_delta(self) -> int | None:
        if self.committed_before is None or self.committed_after is None:
            return None
        return self.committed_after - self.committed_before

    @property
    def end_delta(self) -> int | None:
        if self.end_before is None or self.end_after is None:
            return None
        return self.end_after - self.end_before


@dataclass(frozen=True, slots=True)
class ConsumerProgressReport:
    group: ConsumerGroup
    interval_seconds: float
    partitions: tuple[PartitionProgress, ...]
    status: HealthStatus
    findings: tuple[Finding, ...]
    limitations: tuple[str, ...]

    def count(self, status: ProgressStatus) -> int:
        return sum(1 for p in self.partitions if p.status is status)


def validate_sample_interval(seconds: float) -> float:
    if not MIN_SAMPLE_INTERVAL_SECONDS <= seconds <= MAX_SAMPLE_INTERVAL_SECONDS:
        raise InvalidInputError(
            f"Sampling interval must be between {MIN_SAMPLE_INTERVAL_SECONDS} and "
            f"{MAX_SAMPLE_INTERVAL_SECONDS} seconds."
        )
    return seconds


def detect_stalled_partitions(
    group: ConsumerGroup,
    first: ConsumerGroupLag,
    second: ConsumerGroupLag,
    interval_seconds: float,
    policy: SeverityPolicy | None = None,
) -> ConsumerProgressReport:
    """Compare two lag samples taken ``interval_seconds`` apart."""
    partitions = compare_samples(first, second)
    findings = (policy or SeverityPolicy()).apply(
        [
            *_not_advancing_findings(group, partitions, interval_seconds),
            *_moved_backward_findings(partitions),
            *_data_gap_findings(partitions),
        ]
    )
    has_data = any(p.status is not ProgressStatus.UNKNOWN for p in partitions)
    return ConsumerProgressReport(
        group=group,
        interval_seconds=interval_seconds,
        partitions=partitions,
        status=worst_status(findings, has_data=has_data),
        findings=findings,
        limitations=(
            f"Covers only the {interval_seconds:g}s between two samples; it says "
            "nothing about what happened before or after.",
            "Uses committed offsets, not the consumers' in-memory position. A "
            "consumer that commits less often than the sampling interval can "
            "look stalled; a longer interval reduces that risk.",
        ),
    )


def compare_samples(
    first: ConsumerGroupLag, second: ConsumerGroupLag
) -> tuple[PartitionProgress, ...]:
    before = {(p.topic, p.partition): p for p in first.partitions}
    after = {(p.topic, p.partition): p for p in second.partitions}
    progress = [
        _progress(key, before.get(key), after.get(key))
        for key in sorted(before.keys() | after.keys())
    ]
    return tuple(sorted(progress, key=_progress_order))


def _progress(
    key: tuple[str, int], before: PartitionLag | None, after: PartitionLag | None
) -> PartitionProgress:
    committed_before = before.committed_offset if before else None
    committed_after = after.committed_offset if after else None
    lag_after = after.lag if after else None
    return PartitionProgress(
        topic=key[0],
        partition=key[1],
        committed_before=committed_before,
        committed_after=committed_after,
        end_before=before.log_end_offset if before else None,
        end_after=after.log_end_offset if after else None,
        lag_after=lag_after,
        status=_classify(committed_before, committed_after, lag_after),
    )


def _classify(
    committed_before: int | None, committed_after: int | None, lag_after: int | None
) -> ProgressStatus:
    if committed_before is None or committed_after is None or lag_after is None:
        return ProgressStatus.UNKNOWN
    if committed_after < committed_before:
        return ProgressStatus.MOVED_BACKWARD
    if committed_after > committed_before:
        return ProgressStatus.ADVANCING
    if lag_after == 0:
        return ProgressStatus.CAUGHT_UP
    return ProgressStatus.NOT_ADVANCING


_STATUS_ORDER = {
    ProgressStatus.NOT_ADVANCING: 0,
    ProgressStatus.MOVED_BACKWARD: 1,
    ProgressStatus.UNKNOWN: 2,
    ProgressStatus.ADVANCING: 3,
    ProgressStatus.CAUGHT_UP: 4,
}


def _progress_order(p: PartitionProgress) -> tuple[int, int, str, int]:
    return (_STATUS_ORDER[p.status], -(p.lag_after or 0), p.topic, p.partition)


def _not_advancing_findings(
    group: ConsumerGroup, partitions: tuple[PartitionProgress, ...], interval: float
) -> list[Finding]:
    stalled = [p for p in partitions if p.status is ProgressStatus.NOT_ADVANCING]
    if not stalled:
        return []
    growing = [p for p in stalled if (p.end_delta or 0) > 0]
    hypotheses = [
        "The consumer may be blocked on, or repeatedly failing to process, a "
        "message in these partitions.",
        f"The consumer may commit less often than every {interval:g}s.",
    ]
    if not group.members:
        hypotheses.insert(0, "The group has no active members to consume them.")
    return [
        Finding(
            code=PARTITIONS_NOT_ADVANCING,
            severity=Severity.WARNING,
            summary=(
                f"{len(stalled)} partition(s) had offset lag and their "
                f"committed offset did not change during the {interval:g}s "
                f"sampling window; {len(growing)} of them received new messages "
                "meanwhile."
            ),
            evidence=(
                Evidence("member_count", group.member_count),
                Evidence("backlog_growing_partitions", len(growing)),
                *_progress_evidence(stalled),
            ),
            hypotheses=tuple(hypotheses),
            recommendations=(
                "Sample again with a longer interval to rule out infrequent commits.",
                "Check the consumer application's logs for errors on these partitions.",
            ),
        )
    ]


def _moved_backward_findings(
    partitions: tuple[PartitionProgress, ...],
) -> list[Finding]:
    moved = [p for p in partitions if p.status is ProgressStatus.MOVED_BACKWARD]
    if not moved:
        return []
    return [
        Finding(
            code=COMMITTED_OFFSET_MOVED_BACKWARD,
            severity=Severity.INFO,
            summary=(
                f"The committed offset moved backward on {len(moved)} partition(s) "
                "during the sampling window."
            ),
            evidence=tuple(
                Evidence(f"{p.topic}[{p.partition}].committed_delta", p.committed_delta)
                for p in moved[:MAX_EVIDENCE_PARTITIONS]
            ),
            hypotheses=(
                "Offsets may have been reset, or the consumer may be reprocessing "
                "messages.",
            ),
        )
    ]


def _data_gap_findings(partitions: tuple[PartitionProgress, ...]) -> list[Finding]:
    if not partitions:
        return [
            Finding(
                code=NO_PARTITIONS_SAMPLED,
                severity=Severity.INFO,
                summary=(
                    "Insufficient data: the group has no committed offsets and no "
                    "assigned partitions in either sample."
                ),
            )
        ]
    unknown = [p for p in partitions if p.status is ProgressStatus.UNKNOWN]
    if not unknown:
        return []
    return [
        Finding(
            code=PROGRESS_UNKNOWN,
            severity=Severity.INFO,
            summary=(
                f"Insufficient data: progress is unknown for {len(unknown)} "
                "partition(s), because an offset was missing from at least one "
                "sample."
            ),
            evidence=tuple(
                Evidence(f"{p.topic}[{p.partition}].committed_after", p.committed_after)
                for p in unknown[:MAX_EVIDENCE_PARTITIONS]
            ),
            hypotheses=(
                "The partition may have been assigned or committed for the first "
                "time during the window, or a rebalance may have moved it.",
            ),
        )
    ]


def _progress_evidence(partitions: list[PartitionProgress]) -> list[Evidence]:
    evidence: list[Evidence] = []
    for p in partitions[:MAX_EVIDENCE_PARTITIONS]:
        name = f"{p.topic}[{p.partition}]"
        evidence.append(Evidence(f"{name}.lag", p.lag_after))
        evidence.append(Evidence(f"{name}.new_messages", p.end_delta))
    if len(partitions) > MAX_EVIDENCE_PARTITIONS:
        evidence.append(
            Evidence("more_partitions", len(partitions) - MAX_EVIDENCE_PARTITIONS)
        )
    return evidence
