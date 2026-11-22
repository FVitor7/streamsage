from streamsage.application.ports.consumer_group_reader import ConsumerGroupReader
from streamsage.domain.entities.consumer_group import ConsumerGroup
from streamsage.domain.value_objects.group_id import validate_group_id


class DescribeConsumerGroupUseCase:
    def __init__(self, reader: ConsumerGroupReader) -> None:
        self._reader = reader

    def execute(self, group_id: str) -> ConsumerGroup:
        return self._reader.describe_consumer_group(validate_group_id(group_id))
