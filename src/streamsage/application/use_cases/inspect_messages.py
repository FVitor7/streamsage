from dataclasses import dataclass

from streamsage.application.ports.message_reader import MessageReader
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.messages.planning import (
    plan_from_offset,
    plan_latest,
    readable_partitions,
)
from streamsage.domain.messages.sanitizer import (
    InspectedMessage,
    SanitizationPolicy,
    sanitize_message,
)
from streamsage.domain.value_objects.topic_name import validate_topic_name


@dataclass(frozen=True, slots=True)
class InspectionResult:
    topic: str
    messages: tuple[InspectedMessage, ...]
    payloads_included: bool
    max_payload_bytes: int
    complete: bool
    exact: bool
    """True when the messages are guaranteed to be the newest in the topic
    (or the requested range); False when the read budget, unavailable
    partitions or a timeout limited what was read."""
    messages_read: int
    notes: tuple[str, ...]


class InspectMessagesUseCase:
    """Read a bounded number of messages and return them sanitized.

    Without ``start_offset``, returns the newest ``limit`` messages (across
    the selected partitions, newest first). With ``start_offset`` (which
    requires ``partition``), returns up to ``limit`` messages from there, in
    offset order.

    ``limit`` bounds the output; ``max_read`` bounds how many messages may be
    read to produce it. Only ``limit`` messages are ever held in memory.
    """

    def __init__(
        self,
        reader: MessageReader,
        policy: SanitizationPolicy,
        max_messages: int,
        max_read: int = 10_000,
    ) -> None:
        self._reader = reader
        self._policy = policy
        self._max_messages = max_messages
        self._max_read = max_read

    def execute(
        self,
        topic: str,
        limit: int,
        partition: int | None = None,
        start_offset: int | None = None,
    ) -> InspectionResult:
        name = validate_topic_name(topic)
        self._validate(limit, partition, start_offset)
        offsets = self._reader.partition_offsets(name)
        readable, unavailable, notes_ = readable_partitions(offsets, partition)
        notes = list(notes_)
        if start_offset is not None and partition is not None:
            forward = plan_from_offset(readable, partition, start_offset, limit)
            result = self._reader.read_range(name, forward.ranges)
            ordered = sorted(result.messages, key=lambda m: (m.partition, m.offset))
            exact = True
            notes.extend(forward.notes)
        else:
            latest = plan_latest(readable, limit, self._max_read, partition)
            result = self._reader.read_newest(name, latest.ranges, limit)
            ordered = list(result.messages)
            exact = latest.exact and not unavailable
            notes.extend(latest.notes)
        if not result.complete:
            notes.append(
                "The read stopped before all requested messages were received "
                "(timeout); results are partial."
            )
        return InspectionResult(
            topic=name,
            messages=tuple(sanitize_message(m, self._policy) for m in ordered),
            payloads_included=self._policy.payloads_enabled,
            max_payload_bytes=self._policy.max_payload_bytes,
            complete=result.complete,
            exact=exact and result.complete,
            messages_read=result.messages_read,
            notes=tuple(notes),
        )

    def _validate(
        self, limit: int, partition: int | None, start_offset: int | None
    ) -> None:
        if not 1 <= limit <= self._max_messages:
            raise InvalidInputError(
                f"limit must be between 1 and {self._max_messages}."
            )
        if partition is not None and partition < 0:
            raise InvalidInputError("partition must be zero or greater.")
        if start_offset is not None:
            if partition is None:
                raise InvalidInputError("start_offset requires a partition.")
            if start_offset < 0:
                raise InvalidInputError("start_offset must be zero or greater.")
