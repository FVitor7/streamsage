import time
from collections.abc import Callable

from streamsage.application.ports.consumer_group_reader import ConsumerGroupReader
from streamsage.domain.diagnostics.consumer_progress import (
    ConsumerProgressReport,
    detect_stalled_partitions,
    validate_sample_interval,
)
from streamsage.domain.diagnostics.finding import SeverityPolicy
from streamsage.domain.value_objects.group_id import validate_group_id
from streamsage.domain.value_objects.lag import ConsumerGroupLag


class DetectStalledPartitionsUseCase:
    """Sample a group's lag twice and report partitions that did not advance.

    The two samples are taken within a single call, so no state is stored
    between calls and nothing runs in the background. ``sleep`` is
    injectable for tests.
    """

    def __init__(
        self,
        reader: ConsumerGroupReader,
        policy: SeverityPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._reader = reader
        self._policy = policy
        self._sleep = sleep

    def execute(self, group_id: str, interval_seconds: float) -> ConsumerProgressReport:
        valid_id = validate_group_id(group_id)
        interval = validate_sample_interval(interval_seconds)
        group = self._reader.describe_consumer_group(valid_id)
        first = self._sample(valid_id)
        self._sleep(interval)
        second = self._sample(valid_id)
        return detect_stalled_partitions(group, first, second, interval, self._policy)

    def _sample(self, group_id: str) -> ConsumerGroupLag:
        return ConsumerGroupLag(
            group_id=group_id,
            partitions=tuple(self._reader.get_consumer_lag(group_id)),
        )
