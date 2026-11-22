"""Validation rules for Kafka topic names."""

import re

from streamsage.domain.exceptions import InvalidInputError

MAX_TOPIC_NAME_LENGTH = 249
TOPIC_NAME_PATTERN = r"^[a-zA-Z0-9._-]+$"

_TOPIC_NAME_RE = re.compile(TOPIC_NAME_PATTERN)


def validate_topic_name(name: str) -> str:
    """Return ``name`` if it is a legal Kafka topic name, else raise.

    Rules follow Kafka's own constraints: 1-249 characters from
    ``[a-zA-Z0-9._-]``, and not ``.`` or ``..``.
    """
    if not name:
        raise InvalidInputError("Topic name must not be empty.")
    if len(name) > MAX_TOPIC_NAME_LENGTH:
        raise InvalidInputError(
            f"Topic name must be at most {MAX_TOPIC_NAME_LENGTH} characters."
        )
    if name in {".", ".."}:
        raise InvalidInputError("Topic name cannot be '.' or '..'.")
    if not _TOPIC_NAME_RE.fullmatch(name):
        raise InvalidInputError(
            "Topic name may only contain ASCII letters, digits, '.', '_' and '-'."
        )
    return name
