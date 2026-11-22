from dataclasses import dataclass

from streamsage.domain.diagnostics.severity import Severity

EvidenceValue = str | int | None


@dataclass(frozen=True, slots=True)
class Evidence:
    """One observed value that supports a finding."""

    label: str
    value: EvidenceValue


@dataclass(frozen=True, slots=True)
class Finding:
    """A deterministic observation with the evidence behind it.

    ``code`` is a stable identifier clients can rely on; ``summary`` states
    what was observed, and any possible explanation is worded as a
    hypothesis rather than a conclusion.
    """

    code: str
    severity: Severity
    summary: str
    evidence: tuple[Evidence, ...] = ()
