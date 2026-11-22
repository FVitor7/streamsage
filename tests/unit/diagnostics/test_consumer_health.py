import pytest

from streamsage.domain.diagnostics.consumer_health import (
    MAX_EVIDENCE_PARTITIONS,
    SNAPSHOT_LIMITATION,
    ConsumerHealthReport,
    LagThresholds,
    assess_consumer_health,
)
from streamsage.domain.diagnostics.finding import Evidence
from streamsage.domain.diagnostics.severity import HealthStatus, Severity
from streamsage.domain.entities.consumer_group import (
    ConsumerGroup,
    ConsumerGroupState,
    GroupMember,
    TopicPartitionRef,
)
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.value_objects.lag import ConsumerGroupLag, PartitionLag

THRESHOLDS = LagThresholds(warning=100, critical=1_000)


def _group(
    state: ConsumerGroupState = ConsumerGroupState.STABLE,
    assigned: tuple[tuple[str, int], ...] = (("t", 0), ("t", 1)),
) -> ConsumerGroup:
    members = (
        (
            GroupMember(
                member_id="m-1",
                client_id="c-1",
                host="/10.0.0.1",
                group_instance_id=None,
                assignment=tuple(TopicPartitionRef(t, p) for t, p in assigned),
            ),
        )
        if assigned
        else ()
    )
    return ConsumerGroup("g", state, False, "range", 1, members)


def _lag(*lags: tuple[str, int, int | None]) -> ConsumerGroupLag:
    """(topic, partition, lag) -> PartitionLag with end offset 10_000."""
    return ConsumerGroupLag(
        "g",
        tuple(
            PartitionLag(t, p, None if lag is None else 10_000 - lag, 10_000)
            for t, p, lag in lags
        ),
    )


def _assess(group: ConsumerGroup, lag: ConsumerGroupLag) -> ConsumerHealthReport:
    return assess_consumer_health(group, lag, THRESHOLDS)


def _codes(report: ConsumerHealthReport) -> list[str]:
    return [finding.code for finding in report.findings]


def test_active_group_with_low_lag_is_healthy() -> None:
    report = _assess(_group(), _lag(("t", 0, 10), ("t", 1, 0)))
    assert report.status is HealthStatus.HEALTHY
    assert report.findings == ()
    assert report.limitations[0] == SNAPSHOT_LIMITATION
    assert "warning >= 100" in report.limitations[1]


@pytest.mark.parametrize(
    ("lag", "status", "code"),
    [
        (99, HealthStatus.HEALTHY, None),
        (100, HealthStatus.WARNING, "LAG_ABOVE_WARNING"),  # threshold is inclusive
        (999, HealthStatus.WARNING, "LAG_ABOVE_WARNING"),
        (1_000, HealthStatus.CRITICAL, "LAG_ABOVE_CRITICAL"),
    ],
)
def test_lag_thresholds(lag: int, status: HealthStatus, code: str | None) -> None:
    report = _assess(_group(), _lag(("t", 0, lag), ("t", 1, 0)))
    assert report.status is status
    assert _codes(report) == ([code] if code else [])


def test_critical_finding_lists_offending_partitions_largest_first() -> None:
    report = _assess(_group(), _lag(("t", 0, 1_500), ("t", 1, 5_000)))
    (finding,) = report.findings
    assert finding.severity is Severity.CRITICAL
    assert "2 partition(s)" in finding.summary
    assert finding.evidence == (
        Evidence("max_lag", 5_000),
        Evidence("t[1].lag", 5_000),
        Evidence("t[0].lag", 1_500),
    )


def test_evidence_is_capped() -> None:
    partitions = tuple(("t", p, 2_000 + p) for p in range(MAX_EVIDENCE_PARTITIONS + 3))
    assigned = tuple((t, p) for t, p, _ in partitions)
    report = _assess(_group(assigned=assigned), _lag(*partitions))
    evidence = report.findings[0].evidence
    assert len(evidence) == 1 + MAX_EVIDENCE_PARTITIONS + 1
    assert evidence[-1] == Evidence("more_partitions", 3)


def test_group_without_members_and_pending_messages() -> None:
    report = _assess(
        _group(ConsumerGroupState.EMPTY, assigned=()), _lag(("t", 0, 30), ("t", 1, 20))
    )
    assert report.status is HealthStatus.WARNING
    assert _codes(report) == ["NO_ACTIVE_MEMBERS"]
    assert Evidence("total_lag", 50) in report.findings[0].evidence


def test_inactive_group_that_is_caught_up_is_healthy() -> None:
    report = _assess(_group(ConsumerGroupState.EMPTY, assigned=()), _lag(("t", 0, 0)))
    assert report.status is HealthStatus.HEALTHY
    assert _codes(report) == ["INACTIVE_CAUGHT_UP"]
    assert report.findings[0].severity is Severity.INFO


def test_inactive_group_far_behind_reports_both_findings() -> None:
    report = _assess(
        _group(ConsumerGroupState.EMPTY, assigned=()), _lag(("t", 0, 2_000))
    )
    assert report.status is HealthStatus.CRITICAL
    assert _codes(report) == ["LAG_ABOVE_CRITICAL", "NO_ACTIVE_MEMBERS"]


@pytest.mark.parametrize(
    "state",
    [ConsumerGroupState.PREPARING_REBALANCE, ConsumerGroupState.COMPLETING_REBALANCE],
)
def test_rebalancing_group(state: ConsumerGroupState) -> None:
    report = _assess(_group(state), _lag(("t", 0, 0), ("t", 1, 0)))
    assert report.status is HealthStatus.WARNING
    assert _codes(report) == ["REBALANCING"]
    assert "cannot tell whether it is stuck" in report.findings[0].summary


def test_unassigned_partitions_with_lag() -> None:
    report = _assess(
        _group(assigned=(("t", 0),)),
        _lag(("t", 0, 0), ("old-topic", 0, 40), ("old-topic", 1, 0)),
    )
    assert report.status is HealthStatus.WARNING
    assert _codes(report) == ["UNASSIGNED_PARTITIONS_WITH_LAG"]
    finding = report.findings[0]
    assert "unverified" in finding.summary  # a hypothesis, not a conclusion
    assert finding.evidence == (Evidence("old-topic[0].lag", 40),)


def test_some_unknown_lag_is_reported_without_changing_status() -> None:
    report = _assess(_group(), _lag(("t", 0, 10), ("t", 1, None)))
    assert report.status is HealthStatus.HEALTHY
    assert _codes(report) == ["LAG_UNKNOWN"]
    assert report.findings[0].evidence == (Evidence("t[1].lag", None),)


def test_all_lag_unknown_is_unknown_not_healthy() -> None:
    report = _assess(_group(), _lag(("t", 0, None), ("t", 1, None)))
    assert report.status is HealthStatus.UNKNOWN
    assert _codes(report) == ["LAG_UNKNOWN"]


def test_group_without_any_partition_is_unknown() -> None:
    report = _assess(_group(ConsumerGroupState.EMPTY, assigned=()), _lag())
    assert report.status is HealthStatus.UNKNOWN
    assert _codes(report) == ["NO_PARTITIONS"]


def test_unknown_group_state_is_reported() -> None:
    report = _assess(_group(ConsumerGroupState.UNKNOWN), _lag(("t", 0, 0), ("t", 1, 0)))
    assert report.status is HealthStatus.HEALTHY
    assert _codes(report) == ["STATE_UNKNOWN"]


def test_findings_never_claim_a_consumer_is_stuck() -> None:
    report = _assess(
        _group(ConsumerGroupState.PREPARING_REBALANCE, assigned=(("t", 0),)),
        _lag(("t", 0, 5_000), ("x", 0, 10), ("t", 1, None)),
    )
    for finding in report.findings:
        text = finding.summary.lower()
        assert "is stuck" not in text or "cannot tell whether it is stuck" in text
        assert "root cause" not in text


@pytest.mark.parametrize(("warning", "critical"), [(0, 10), (10, 9), (-1, -1)])
def test_invalid_thresholds(warning: int, critical: int) -> None:
    with pytest.raises(InvalidInputError):
        LagThresholds(warning=warning, critical=critical)


def test_equal_thresholds_are_allowed() -> None:
    assert LagThresholds(warning=10, critical=10).critical == 10
