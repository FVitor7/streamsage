from dataclasses import dataclass

from streamsage.application.ports.message_reader import MessageReader
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.messages.planning import ScanCoverage, plan_key_scan
from streamsage.domain.messages.sanitizer import (
    InspectedMessage,
    SanitizationPolicy,
    sanitize_message,
)
from streamsage.domain.value_objects.topic_name import validate_topic_name

MAX_SEARCH_KEY_BYTES = 1024


@dataclass(frozen=True, slots=True)
class KeySearchLimits:
    max_results: int
    max_scan_per_partition: int
    max_scan_total: int

    def __post_init__(self) -> None:
        for name in ("max_results", "max_scan_per_partition", "max_scan_total"):
            if getattr(self, name) < 1:
                raise InvalidInputError(f"{name} must be at least 1.")


@dataclass(frozen=True, slots=True)
class KeySearchResult:
    topic: str
    matches: tuple[InspectedMessage, ...]
    scanned: int
    per_partition_limit: int
    coverage: tuple[ScanCoverage, ...]
    complete: bool
    notes: tuple[str, ...]

    @property
    def searched_whole_topic(self) -> bool:
        return self.complete and all(c.reaches_start for c in self.coverage)


class FindMessagesByKeyUseCase:
    """Find the newest messages with an exact key, within scan budgets.

    Kafka has no index by key, so this scans the newest messages of each
    partition (bounded per partition and in total) and reports exactly what
    was covered. "No match" therefore means "no match in the scanned range".
    """

    def __init__(
        self,
        reader: MessageReader,
        policy: SanitizationPolicy,
        limits: KeySearchLimits,
    ) -> None:
        self._reader = reader
        self._policy = policy
        self._limits = limits

    def execute(
        self, topic: str, key: str, limit: int, partition: int | None = None
    ) -> KeySearchResult:
        name = validate_topic_name(topic)
        key_bytes = self._validate(key, limit, partition)
        offsets = self._reader.partition_offsets(name)
        plan = plan_key_scan(
            offsets,
            self._limits.max_scan_per_partition,
            self._limits.max_scan_total,
            partition,
        )
        scan = self._reader.find_by_key(name, plan.ranges, key_bytes, limit)
        newest = sorted(
            scan.matches, key=lambda m: (m.timestamp_ms or 0, m.offset), reverse=True
        )[:limit]

        notes = []
        partial = [c.partition for c in plan.coverage if not c.reaches_start]
        if partial:
            notes.append(
                f"Only the newest {plan.per_partition_limit} message(s) of each "
                f"partition were searched; older messages in partition(s) "
                f"{', '.join(map(str, partial))} were not. No match does not "
                "mean the key is absent."
            )
        if not scan.complete:
            notes.append(
                "The scan stopped before covering all planned messages "
                "(timeout); results are partial."
            )
        return KeySearchResult(
            topic=name,
            matches=tuple(sanitize_message(m, self._policy) for m in newest),
            scanned=scan.scanned,
            per_partition_limit=plan.per_partition_limit,
            coverage=plan.coverage,
            complete=scan.complete,
            notes=tuple(notes),
        )

    def _validate(self, key: str, limit: int, partition: int | None) -> bytes:
        key_bytes = key.encode("utf-8")
        if not key_bytes:
            raise InvalidInputError("key must not be empty.")
        if len(key_bytes) > MAX_SEARCH_KEY_BYTES:
            raise InvalidInputError(
                f"key must be at most {MAX_SEARCH_KEY_BYTES} bytes."
            )
        if not 1 <= limit <= self._limits.max_results:
            raise InvalidInputError(
                f"limit must be between 1 and {self._limits.max_results}."
            )
        if partition is not None and partition < 0:
            raise InvalidInputError("partition must be zero or greater.")
        return key_bytes
