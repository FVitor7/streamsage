from collections.abc import Collection
from dataclasses import replace

from streamsage.application.ports.consumer_group_reader import ConsumerGroupReader
from streamsage.domain.entities.consumer_group import (
    ConsumerGroupListing,
    ConsumerGroupState,
)


class ListConsumerGroupsUseCase:
    """List groups sorted by id, optionally filtered by state.

    Completeness information from the reader is preserved, so callers never
    present an incomplete listing as exhaustive.
    """

    def __init__(self, reader: ConsumerGroupReader) -> None:
        self._reader = reader

    def execute(
        self, states: Collection[ConsumerGroupState] | None = None
    ) -> ConsumerGroupListing:
        listing = self._reader.list_consumer_groups()
        groups = listing.groups
        if states:
            wanted = set(states)
            groups = tuple(group for group in groups if group.state in wanted)
        return replace(listing, groups=tuple(sorted(groups, key=lambda g: g.group_id)))
