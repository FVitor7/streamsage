import pytest

from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.value_objects.topic_name import (
    MAX_TOPIC_NAME_LENGTH,
    validate_topic_name,
)


@pytest.mark.parametrize(
    "name",
    [
        "orders.created",
        "a",
        "A-b_c.9",
        "__consumer_offsets",
        "x" * MAX_TOPIC_NAME_LENGTH,
    ],
)
def test_valid_topic_names(name: str) -> None:
    assert validate_topic_name(name) == name


@pytest.mark.parametrize(
    "name",
    [
        "",
        ".",
        "..",
        "has space",
        "orders/created",
        "../etc/passwd",
        "tópico",
        "x" * (MAX_TOPIC_NAME_LENGTH + 1),
        "name\n",
    ],
)
def test_invalid_topic_names(name: str) -> None:
    with pytest.raises(InvalidInputError):
        validate_topic_name(name)
