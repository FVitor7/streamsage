from streamsage.application.ports.consumer_group_reader import ConsumerGroupReader
from streamsage.domain.value_objects.group_id import validate_group_id
from streamsage.domain.value_objects.lag import ConsumerGroupLag


class GetConsumerLagUseCase:
    """Per-partition lag of a group, largest first."""

    def __init__(self, reader: ConsumerGroupReader) -> None:
        self._reader = reader

    def execute(self, group_id: str) -> ConsumerGroupLag:
        valid_id = validate_group_id(group_id)
        return ConsumerGroupLag(
            group_id=valid_id,
            partitions=tuple(self._reader.get_consumer_lag(valid_id)),
        )
