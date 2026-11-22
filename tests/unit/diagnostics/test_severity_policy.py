import pytest

from streamsage.domain.diagnostics.catalog import ALL_FINDING_CODES, severity_policy
from streamsage.domain.diagnostics.consumer_health import (
    LagThresholds,
    assess_consumer_health,
)
from streamsage.domain.diagnostics.finding import Finding, SeverityPolicy, worst_status
from streamsage.domain.diagnostics.severity import HealthStatus, Severity
from streamsage.domain.entities.consumer_group import ConsumerGroup, ConsumerGroupState
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.value_objects.lag import ConsumerGroupLag, PartitionLag


def test_policy_replaces_severity_by_code_only() -> None:
    policy = SeverityPolicy({"A": Severity.INFO}, known_codes={"A", "B"})
    findings = policy.apply(
        [Finding("A", Severity.CRITICAL, "a"), Finding("B", Severity.WARNING, "b")]
    )
    assert [f.severity for f in findings] == [Severity.INFO, Severity.WARNING]
    assert findings[0].summary == "a"


def test_policy_rejects_unknown_codes() -> None:
    with pytest.raises(InvalidInputError, match="TYPO"):
        SeverityPolicy({"TYPO": Severity.INFO}, known_codes={"A"})


def test_policy_overrides_are_read_only() -> None:
    policy = SeverityPolicy({"A": Severity.INFO}, known_codes={"A"})
    with pytest.raises(TypeError):
        policy.overrides["A"] = Severity.CRITICAL  # type: ignore[index]


def test_catalog_policy_knows_every_code() -> None:
    assert {"NO_ACTIVE_MEMBERS", "PARTITIONS_NOT_ADVANCING"} <= ALL_FINDING_CODES
    policy = severity_policy({code: Severity.INFO for code in ALL_FINDING_CODES})
    assert len(policy.overrides) == len(ALL_FINDING_CODES)


def test_policy_can_downgrade_a_health_finding() -> None:
    group = ConsumerGroup("g", ConsumerGroupState.EMPTY, True, None, None, ())
    lag = ConsumerGroupLag("g", (PartitionLag("t", 0, 0, 10),))
    thresholds = LagThresholds(warning=100, critical=1_000)
    default = assess_consumer_health(group, lag, thresholds)
    relaxed = assess_consumer_health(
        group, lag, thresholds, severity_policy({"NO_ACTIVE_MEMBERS": Severity.INFO})
    )
    assert default.status is HealthStatus.WARNING
    assert relaxed.status is HealthStatus.HEALTHY


@pytest.mark.parametrize(
    ("severities", "has_data", "expected"),
    [
        ((), True, HealthStatus.HEALTHY),
        ((), False, HealthStatus.UNKNOWN),
        ((Severity.INFO,), False, HealthStatus.UNKNOWN),
        ((Severity.INFO, Severity.WARNING), False, HealthStatus.WARNING),
        ((Severity.WARNING, Severity.CRITICAL), True, HealthStatus.CRITICAL),
    ],
)
def test_worst_status(
    severities: tuple[Severity, ...], has_data: bool, expected: HealthStatus
) -> None:
    findings = [Finding("X", s, "x") for s in severities]
    assert worst_status(findings, has_data=has_data) is expected
