from dataclasses import dataclass

from streamsage.application.ports.message_reader import MessageReader
from streamsage.application.ports.topic_reader import TopicReader
from streamsage.domain.messages.dlq import DlqTopicMatcher

MAX_DLQ_TOPICS = 50


@dataclass(frozen=True, slots=True)
class DlqTopicSummary:
    name: str
    partition_count: int
    message_count: int
    """Upper bound (high - low watermark); compaction can leave gaps."""


@dataclass(frozen=True, slots=True)
class DlqOverview:
    configured: bool
    patterns: tuple[str, ...]
    topics: tuple[DlqTopicSummary, ...]
    notes: tuple[str, ...]


class ListDlqTopicsUseCase:
    """List existing topics that match the configured DLQ patterns."""

    def __init__(
        self,
        topic_reader: TopicReader,
        message_reader: MessageReader,
        matcher: DlqTopicMatcher,
    ) -> None:
        self._topic_reader = topic_reader
        self._message_reader = message_reader
        self._matcher = matcher

    def execute(self) -> DlqOverview:
        if not self._matcher.configured:
            return DlqOverview(
                configured=False,
                patterns=(),
                topics=(),
                notes=(
                    "No DLQ topics are configured (STREAMSAGE_DLQ_TOPICS). "
                    "StreamSage does not assume a DLQ naming convention.",
                ),
            )
        names = sorted(
            topic.name
            for topic in self._topic_reader.list_topics()
            if not topic.internal and self._matcher.matches(topic.name)
        )
        notes = []
        if not names:
            notes.append("No existing topic matches the configured DLQ patterns.")
        if len(names) > MAX_DLQ_TOPICS:
            notes.append(
                f"{len(names)} topics match; only the first {MAX_DLQ_TOPICS} "
                "(alphabetically) are summarized."
            )
            names = names[:MAX_DLQ_TOPICS]
        topics = []
        for name in names:
            offsets = self._message_reader.partition_offsets(name)
            topics.append(
                DlqTopicSummary(
                    name=name,
                    partition_count=offsets.partition_count,
                    message_count=sum(p.message_count for p in offsets.partitions),
                )
            )
            if offsets.unavailable_partitions:
                notes.append(
                    f"{name}: offsets of {len(offsets.unavailable_partitions)} "
                    "partition(s) are unavailable; its message count only covers "
                    "the other partitions."
                )
        topics.sort(key=lambda t: (-t.message_count, t.name))
        return DlqOverview(
            configured=True,
            patterns=self._matcher.patterns,
            topics=tuple(topics),
            notes=tuple(notes),
        )
