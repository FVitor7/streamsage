from typing import Protocol, runtime_checkable

from streamsage.domain.entities.cluster import Cluster


@runtime_checkable
class ClusterReader(Protocol):
    """Read-only access to cluster-level metadata."""

    def get_cluster_info(self) -> Cluster: ...
