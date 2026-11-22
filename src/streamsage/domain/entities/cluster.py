"""Cluster-level domain entities."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Broker:
    broker_id: int
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class Cluster:
    """Cluster metadata as observed by a single metadata request.

    ``cluster_id`` and ``controller_id`` are ``None`` when the cluster does not
    report them. Unknown values are never replaced with placeholders.
    """

    cluster_id: str | None
    brokers: tuple[Broker, ...]
    controller_id: int | None

    @property
    def broker_count(self) -> int:
        return len(self.brokers)
