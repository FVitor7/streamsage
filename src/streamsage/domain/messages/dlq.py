"""Recognition of dead letter queue (DLQ) topics.

There is no universal DLQ naming convention (``orders.DLQ``,
``dlq-orders``, ``orders-dead-letter``, ...), so StreamSage treats a topic
as a DLQ only when it matches an operator-configured exact name or glob
pattern.
"""

import re
from collections.abc import Iterable
from fnmatch import fnmatchcase

from streamsage.domain.exceptions import InvalidInputError

# Topic name characters plus glob syntax.
_PATTERN = re.compile(r"^[a-zA-Z0-9._\-*?\[\]!]+$")


class DlqTopicMatcher:
    def __init__(self, patterns: Iterable[str] = ()) -> None:
        cleaned = tuple(dict.fromkeys(p.strip() for p in patterns if p.strip()))
        invalid = [p for p in cleaned if not _PATTERN.fullmatch(p)]
        if invalid:
            raise InvalidInputError(
                "Invalid DLQ topic pattern(s): " + ", ".join(invalid)
            )
        self._patterns = cleaned

    @property
    def patterns(self) -> tuple[str, ...]:
        return self._patterns

    @property
    def configured(self) -> bool:
        return bool(self._patterns)

    def matches(self, topic: str) -> bool:
        return any(fnmatchcase(topic, pattern) for pattern in self._patterns)
