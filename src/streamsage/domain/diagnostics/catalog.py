"""Registry of every finding code StreamSage can emit."""

from collections.abc import Mapping

from streamsage.domain.diagnostics.consumer_health import HEALTH_FINDING_CODES
from streamsage.domain.diagnostics.consumer_progress import PROGRESS_FINDING_CODES
from streamsage.domain.diagnostics.finding import SeverityPolicy
from streamsage.domain.diagnostics.severity import Severity

ALL_FINDING_CODES = HEALTH_FINDING_CODES | PROGRESS_FINDING_CODES


def severity_policy(overrides: Mapping[str, Severity] | None = None) -> SeverityPolicy:
    """A policy validated against every known finding code."""
    return SeverityPolicy(overrides, known_codes=ALL_FINDING_CODES)
