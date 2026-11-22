from collections.abc import Collection

from streamsage.application.ports.consumer_group_reader import ConsumerGroupReader
from streamsage.domain.entities.consumer_group import (
    ConsumerGroupState,
    ConsumerGroupSummary,
)


class ListConsumerGroupsUseCase:
    """List groups sorted by id, optionally filtered by state."""

    def __init__(self, reader: ConsumerGroupReader) -> None:
        self._reader = reader

    def execute(
        self, states: Collection[ConsumerGroupState] | None = None
    ) -> list[ConsumerGroupSummary]:
        groups = self._reader.list_consumer_groups()
        if states:
            wanted = set(states)
            groups = [group for group in groups if group.state in wanted]
        return sorted(groups, key=lambda group: group.group_id)
