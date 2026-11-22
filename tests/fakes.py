"""In-memory test doubles for the application ports."""

import secrets

from streamsage.domain.entities.cluster import Broker, Cluster
from streamsage.domain.entities.topic import Partition, Topic
from streamsage.domain.exceptions import KafkaResourceNotFoundError


def make_topic(
    name: str,
    partitions: int = 1,
    *,
    replicas: tuple[int, ...] = (1,),
    isr: tuple[int, ...] | None = None,
    leader: int | None = 1,
    internal: bool | None = None,
) -> Topic:
    return Topic(
        name=name,
        partitions=tuple(
            Partition(
                partition_id=i,
                leader=leader,
                replicas=replicas,
                isr=replicas if isr is None else isr,
            )
            for i in range(partitions)
        ),
        internal=name.startswith("__") if internal is None else internal,
    )


def random_credential() -> str:
    """Generate a throwaway credential per test run.

    Tests never hardcode credential-looking literals, so secret scanners have
    nothing to flag and leak assertions cannot pass by coincidence.
    """
    return secrets.token_urlsafe(24)


SAMPLE_CLUSTER = Cluster(
    cluster_id="test-cluster",
    brokers=(Broker(1, "broker-1", 9092), Broker(2, "broker-2", 9092)),
    controller_id=1,
)


class FakeClusterReader:
    def __init__(
        self, cluster: Cluster = SAMPLE_CLUSTER, error: Exception | None = None
    ) -> None:
        self.cluster = cluster
        self.error = error

    def get_cluster_info(self) -> Cluster:
        if self.error is not None:
            raise self.error
        return self.cluster


class FakeTopicReader:
    def __init__(
        self, topics: list[Topic] | None = None, error: Exception | None = None
    ) -> None:
        self.topics = topics if topics is not None else []
        self.error = error
        self.described: list[str] = []

    def list_topics(self) -> list[Topic]:
        if self.error is not None:
            raise self.error
        return list(self.topics)

    def describe_topic(self, name: str) -> Topic:
        self.described.append(name)
        if self.error is not None:
            raise self.error
        for topic in self.topics:
            if topic.name == name:
                return topic
        raise KafkaResourceNotFoundError(f"Topic '{name}' does not exist.")
