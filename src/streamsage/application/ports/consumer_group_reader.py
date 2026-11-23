from typing import Protocol, runtime_checkable

from streamsage.domain.entities.consumer_group import (
    ConsumerGroup,
    ConsumerGroupListing,
)
from streamsage.domain.value_objects.lag import PartitionLag


@runtime_checkable
class ConsumerGroupReader(Protocol):
    """Read-only access to consumer groups and their offsets."""

    def list_consumer_groups(self) -> ConsumerGroupListing:
        """All groups, with ``failed_brokers`` > 0 if the listing is partial.

        Raises if no broker answered at all.
        """
        ...

    def describe_consumer_group(self, group_id: str) -> ConsumerGroup:
        """Return one group or raise ``KafkaResourceNotFoundError``."""
        ...

    def get_consumer_lag(self, group_id: str) -> list[PartitionLag]:
        """Lag for every partition the group has committed or is assigned.

        Raises ``KafkaResourceNotFoundError`` if the group does not exist.
        """
        ...
