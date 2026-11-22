import pytest

from streamsage.domain.diagnostics.consumer_progress import (
    MAX_SAMPLE_INTERVAL_SECONDS,
    MIN_SAMPLE_INTERVAL_SECONDS,
    ConsumerProgressReport,
    ProgressStatus,
    detect_stalled_partitions,
    validate_sample_interval,
)
from streamsage.domain.diagnostics.finding import Evidence, SeverityPolicy
from streamsage.domain.diagnostics.severity import HealthStatus, Severity
from streamsage.domain.entities.consumer_group import (
    ConsumerGroup,
    ConsumerGroupState,
    GroupMember,
)
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.value_objects.lag import ConsumerGroupLag, PartitionLag

ACTIVE = ConsumerGroup(
    "g",
    ConsumerGroupState.STABLE,
    False,
    "range",
    1,
    (GroupMember("m-1", "c-1", "/10.0.0.1", None, ()),),
)
INACTIVE = ConsumerGroup("g", ConsumerGroupState.EMPTY, True, None, 1, ())

Offsets = tuple[str, int, int | None, int | None]  # topic, partition, committed, end


def _sample(*offsets: Offsets) -> ConsumerGroupLag:
    return ConsumerGroupLag(
        "g", tuple(PartitionLag(t, p, c, e) for t, p, c, e in offsets)
    )


def _detect(
    first: ConsumerGroupLag,
    second: ConsumerGroupLag,
    group: ConsumerGroup = ACTIVE,
    policy: SeverityPolicy | None = None,
) -> ConsumerProgressReport:
    return detect_stalled_partitions(group, first, second, 10, policy)


def _statuses(report: ConsumerProgressReport) -> dict[tuple[str, int], ProgressStatus]:
    return {(p.topic, p.partition): p.status for p in report.partitions}


@pytest.mark.parametrize(
    ("before", "after", "expected"),
    [
        ((40, 100), (60, 120), ProgressStatus.ADVANCING),
        ((100, 100), (100, 100), ProgressStatus.CAUGHT_UP),
        ((40, 100), (40, 100), ProgressStatus.NOT_ADVANCING),  # backlog, idle producer
        ((40, 100), (40, 150), ProgressStatus.NOT_ADVANCING),  # backlog growing
        ((60, 100), (10, 100), ProgressStatus.MOVED_BACKWARD),
        ((None, 100), (40, 100), ProgressStatus.UNKNOWN),  # first commit in window
        ((40, 100), (40, None), ProgressStatus.UNKNOWN),
    ],
)
def test_partition_classification(
    before: tuple[int | None, int | None],
    after: tuple[int | None, int | None],
    expected: ProgressStatus,
) -> None:
    report = _detect(_sample(("t", 0, *before)), _sample(("t", 0, *after)))
    assert _statuses(report) == {("t", 0): expected}


def test_advancing_group_is_healthy() -> None:
    report = _detect(
        _sample(("t", 0, 40, 100), ("t", 1, 100, 100)),
        _sample(("t", 0, 80, 130), ("t", 1, 100, 100)),
    )
    assert report.status is HealthStatus.HEALTHY
    assert report.findings == ()
    assert report.count(ProgressStatus.ADVANCING) == 1
    assert report.count(ProgressStatus.CAUGHT_UP) == 1
    progress = {p.partition: p for p in report.partitions}[0]
    assert (progress.committed_delta, progress.end_delta) == (40, 30)


def test_not_advancing_partitions_are_reported_with_evidence() -> None:
    report = _detect(
        _sample(("t", 0, 40, 100), ("t", 1, 10, 50), ("t", 2, 5, 5)),
        _sample(("t", 0, 40, 160), ("t", 1, 10, 50), ("t", 2, 5, 5)),
    )
    assert report.status is HealthStatus.WARNING
    (finding,) = report.findings
    assert finding.code == "PARTITIONS_NOT_ADVANCING"
    assert finding.severity is Severity.WARNING
    assert "2 partition(s)" in finding.summary
    assert "1 of them received new messages" in finding.summary
    assert finding.evidence == (
        Evidence("member_count", 1),
        Evidence("backlog_growing_partitions", 1),
        Evidence("t[0].lag", 120),
        Evidence("t[0].new_messages", 60),
        Evidence("t[1].lag", 40),
        Evidence("t[1].new_messages", 0),
    )
    assert any("commit less often than every 10s" in h for h in finding.hypotheses)
    assert finding.recommendations
    # Not-advancing partitions are listed first.
    assert [p.status for p in report.partitions][:2] == [
        ProgressStatus.NOT_ADVANCING
    ] * 2


def test_inactive_group_hypothesis_mentions_missing_members() -> None:
    report = _detect(
        _sample(("t", 0, 40, 100)), _sample(("t", 0, 40, 100)), group=INACTIVE
    )
    (finding,) = report.findings
    assert finding.hypotheses[0] == "The group has no active members to consume them."


def test_findings_do_not_assert_a_root_cause() -> None:
    report = _detect(_sample(("t", 0, 40, 100)), _sample(("t", 0, 40, 200)))
    for finding in report.findings:
        assert "root cause" not in finding.summary.lower()
        assert "stuck" not in finding.summary.lower()
        assert all(h.startswith("The ") for h in finding.hypotheses)


def test_moved_backward_is_informational() -> None:
    report = _detect(_sample(("t", 0, 60, 100)), _sample(("t", 0, 10, 100)))
    assert report.status is HealthStatus.HEALTHY
    (finding,) = report.findings
    assert finding.code == "COMMITTED_OFFSET_MOVED_BACKWARD"
    assert finding.evidence == (Evidence("t[0].committed_delta", -50),)


def test_partition_present_in_only_one_sample_is_unknown() -> None:
    report = _detect(
        _sample(("t", 0, 40, 100), ("t", 1, 5, 9)),
        _sample(("t", 0, 50, 100)),
    )
    assert _statuses(report)[("t", 1)] is ProgressStatus.UNKNOWN
    assert [f.code for f in report.findings] == ["PROGRESS_UNKNOWN"]
    assert report.status is HealthStatus.HEALTHY  # t-0 advanced


def test_only_unknown_progress_is_unknown_status() -> None:
    report = _detect(_sample(("t", 0, None, 100)), _sample(("t", 0, None, 100)))
    assert report.status is HealthStatus.UNKNOWN


def test_no_partitions_in_either_sample() -> None:
    report = _detect(_sample(), _sample(), group=INACTIVE)
    assert report.status is HealthStatus.UNKNOWN
    assert [f.code for f in report.findings] == ["NO_PARTITIONS_SAMPLED"]


def test_limitations_mention_window_and_commit_frequency() -> None:
    report = _detect(_sample(("t", 0, 1, 1)), _sample(("t", 0, 1, 1)))
    assert "10s" in report.limitations[0]
    assert "commits less often" in report.limitations[1]


def test_severity_policy_overrides_status() -> None:
    policy = SeverityPolicy(
        {"PARTITIONS_NOT_ADVANCING": Severity.CRITICAL},
        known_codes={"PARTITIONS_NOT_ADVANCING"},
    )
    report = _detect(
        _sample(("t", 0, 40, 100)), _sample(("t", 0, 40, 100)), policy=policy
    )
    assert report.findings[0].severity is Severity.CRITICAL
    assert report.status is HealthStatus.CRITICAL


@pytest.mark.parametrize(
    "seconds", [MIN_SAMPLE_INTERVAL_SECONDS, 10, MAX_SAMPLE_INTERVAL_SECONDS]
)
def test_valid_sample_intervals(seconds: int) -> None:
    assert validate_sample_interval(seconds) == seconds


@pytest.mark.parametrize("seconds", [0, 0.5, MAX_SAMPLE_INTERVAL_SECONDS + 1, -1])
def test_invalid_sample_intervals(seconds: float) -> None:
    with pytest.raises(InvalidInputError):
        validate_sample_interval(seconds)
