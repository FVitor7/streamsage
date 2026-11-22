from dataclasses import replace

from streamsage.application.ports.topic_reader import TopicReader
from streamsage.domain.entities.topic import Topic
from streamsage.domain.value_objects.topic_name import validate_topic_name


class DescribeTopicUseCase:
    """Describe one topic, with partitions ordered by partition id."""

    def __init__(self, topic_reader: TopicReader) -> None:
        self._topic_reader = topic_reader

    def execute(self, topic_name: str) -> Topic:
        name = validate_topic_name(topic_name)
        topic = self._topic_reader.describe_topic(name)
        ordered = tuple(sorted(topic.partitions, key=lambda p: p.partition_id))
        return replace(topic, partitions=ordered)
