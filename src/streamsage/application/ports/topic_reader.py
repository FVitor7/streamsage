from typing import Protocol, runtime_checkable

from streamsage.domain.entities.topic import Topic


@runtime_checkable
class TopicReader(Protocol):
    """Read-only access to topic metadata.

    Implementations must never create topics as a side effect, including when
    ``describe_topic`` is called with a name that does not exist.
    """

    def list_topics(self) -> list[Topic]:
        """Return every topic visible to the configured credentials."""
        ...

    def describe_topic(self, name: str) -> Topic:
        """Return one topic or raise ``KafkaResourceNotFoundError``."""
        ...
