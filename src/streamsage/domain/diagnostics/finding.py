from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from streamsage.domain.diagnostics.severity import HealthStatus, Severity
from streamsage.domain.exceptions import InvalidInputError

EvidenceValue = str | int | None


@dataclass(frozen=True, slots=True)
class Evidence:
    """One observed value that supports a finding."""

    label: str
    value: EvidenceValue


@dataclass(frozen=True, slots=True)
class Finding:
    """A deterministic observation with the evidence behind it.

    - ``code`` is a stable identifier clients can rely on.
    - ``summary`` states only what was observed.
    - ``hypotheses`` are possible explanations. They are unverified by
      definition; no finding asserts a root cause.
    - ``recommendations`` are read-only next steps for a human or agent.
    """

    code: str
    severity: Severity
    summary: str
    evidence: tuple[Evidence, ...] = ()
    hypotheses: tuple[str, ...] = ()
    recommendations: tuple[str, ...] = ()


class SeverityPolicy:
    """Overrides the default severity of findings by code.

    Teams differ on what deserves attention: an inactive consumer group may
    be critical for one and expected for another. Unknown codes are
    rejected so a typo cannot silently disable an override.
    """

    def __init__(
        self,
        overrides: Mapping[str, Severity] | None = None,
        *,
        known_codes: Iterable[str] = (),
    ) -> None:
        overrides = dict(overrides or {})
        known = frozenset(known_codes)
        unknown = sorted(set(overrides) - known)
        if unknown:
            raise InvalidInputError(
                "Unknown finding code(s) in severity overrides: " + ", ".join(unknown)
            )
        self._overrides = MappingProxyType(overrides)

    @property
    def overrides(self) -> Mapping[str, Severity]:
        return self._overrides

    def apply(self, findings: Iterable[Finding]) -> tuple[Finding, ...]:
        return tuple(
            replace(finding, severity=self._overrides[finding.code])
            if finding.code in self._overrides
            else finding
            for finding in findings
        )


def worst_status(findings: Iterable[Finding], *, has_data: bool) -> HealthStatus:
    """Overall status: the worst severity, or UNKNOWN without data.

    Without any warning or critical finding, the status is ``UNKNOWN`` when
    there was not enough data to judge, and ``HEALTHY`` otherwise.
    """
    severities = {finding.severity for finding in findings}
    if Severity.CRITICAL in severities:
        return HealthStatus.CRITICAL
    if Severity.WARNING in severities:
        return HealthStatus.WARNING
    return HealthStatus.HEALTHY if has_data else HealthStatus.UNKNOWN
