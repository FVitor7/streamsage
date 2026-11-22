"""Validation rules for consumer group ids."""

from streamsage.domain.exceptions import InvalidInputError

MAX_GROUP_ID_LENGTH = 255


def validate_group_id(group_id: str) -> str:
    """Return ``group_id`` if it is acceptable, else raise.

    Kafka accepts almost any string as a group id. StreamSage additionally
    rejects empty, overly long, or non-printable ids, which are almost
    certainly mistakes and would make logs and responses ambiguous.
    """
    if not group_id or not group_id.strip():
        raise InvalidInputError("Consumer group id must not be empty.")
    if len(group_id) > MAX_GROUP_ID_LENGTH:
        raise InvalidInputError(
            f"Consumer group id must be at most {MAX_GROUP_ID_LENGTH} characters."
        )
    if not group_id.isprintable():
        raise InvalidInputError(
            "Consumer group id must not contain control characters."
        )
    return group_id
