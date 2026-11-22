from enum import StrEnum


class Severity(StrEnum):
    """Severity of a single finding."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class HealthStatus(StrEnum):
    """Overall classification of an analyzed resource.

    ``UNKNOWN`` means there was not enough data to classify, which is
    different from ``HEALTHY``.
    """

    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"
