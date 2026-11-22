from streamsage.application.ports.topic_reader import TopicReader
from streamsage.domain.entities.topic import Topic


class ListTopicsUseCase:
    """List topics sorted by name, hiding internal topics unless requested."""

    def __init__(self, topic_reader: TopicReader) -> None:
        self._topic_reader = topic_reader

    def execute(self, include_internal: bool = False) -> list[Topic]:
        topics = self._topic_reader.list_topics()
        if not include_internal:
            topics = [topic for topic in topics if not topic.internal]
        return sorted(topics, key=lambda topic: topic.name)
