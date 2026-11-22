from streamsage.application.ports.consumer_group_reader import ConsumerGroupReader
from streamsage.domain.diagnostics.consumer_health import (
    ConsumerHealthReport,
    LagThresholds,
    assess_consumer_health,
)
from streamsage.domain.diagnostics.finding import SeverityPolicy
from streamsage.domain.value_objects.group_id import validate_group_id
from streamsage.domain.value_objects.lag import ConsumerGroupLag


class AnalyzeConsumerHealthUseCase:
    """Classify a consumer group's health from its state and current lag."""

    def __init__(
        self,
        reader: ConsumerGroupReader,
        thresholds: LagThresholds,
        policy: SeverityPolicy | None = None,
    ) -> None:
        self._reader = reader
        self._thresholds = thresholds
        self._policy = policy

    def execute(self, group_id: str) -> ConsumerHealthReport:
        valid_id = validate_group_id(group_id)
        group = self._reader.describe_consumer_group(valid_id)
        lag = ConsumerGroupLag(
            group_id=valid_id,
            partitions=tuple(self._reader.get_consumer_lag(valid_id)),
        )
        return assess_consumer_health(group, lag, self._thresholds, self._policy)
