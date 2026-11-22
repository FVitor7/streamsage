from streamsage.application.ports.cluster_reader import ClusterReader
from streamsage.domain.entities.cluster import Cluster


class GetClusterInfoUseCase:
    def __init__(self, cluster_reader: ClusterReader) -> None:
        self._cluster_reader = cluster_reader

    def execute(self) -> Cluster:
        return self._cluster_reader.get_cluster_info()
