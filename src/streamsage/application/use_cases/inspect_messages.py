from dataclasses import dataclass

from streamsage.application.ports.message_reader import MessageReader
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.messages.planning import plan_from_offset, plan_latest
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
    notes: tuple[str, ...]


class InspectMessagesUseCase:
    """Read a bounded number of messages and return them sanitized.

    Without ``start_offset``, returns the newest ``limit`` messages (across
    the selected partitions, newest first). With ``start_offset`` (which
    requires ``partition``), returns up to ``limit`` messages from there, in
    offset order.
    """

    def __init__(
        self, reader: MessageReader, policy: SanitizationPolicy, max_messages: int
    ) -> None:
        self._reader = reader
        self._policy = policy
        self._max_messages = max_messages

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
        if start_offset is not None and partition is not None:
            plan = plan_from_offset(offsets, partition, start_offset, limit)
        else:
            plan = plan_latest(offsets, limit, partition)
        result = self._reader.read_range(name, plan.ranges)

        if start_offset is None:
            ordered = sorted(
                result.messages,
                key=lambda m: (m.timestamp_ms or 0, m.offset),
                reverse=True,
            )[:limit]
        else:
            ordered = sorted(result.messages, key=lambda m: (m.partition, m.offset))
        notes = list(plan.notes)
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
