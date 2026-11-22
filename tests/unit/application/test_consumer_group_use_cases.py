import pytest

from streamsage.application.use_cases.analyze_consumer_health import (
    AnalyzeConsumerHealthUseCase,
)
from streamsage.application.use_cases.describe_consumer_group import (
    DescribeConsumerGroupUseCase,
)
from streamsage.application.use_cases.detect_stalled_partitions import (
    DetectStalledPartitionsUseCase,
)
from streamsage.application.use_cases.get_consumer_lag import GetConsumerLagUseCase
from streamsage.application.use_cases.list_consumer_groups import (
    ListConsumerGroupsUseCase,
)
from streamsage.domain.diagnostics.consumer_health import LagThresholds
from streamsage.domain.diagnostics.severity import HealthStatus
from streamsage.domain.entities.consumer_group import ConsumerGroupState
from streamsage.domain.exceptions import (
    InvalidInputError,
    KafkaConnectionError,
    KafkaResourceNotFoundError,
)
from streamsage.domain.value_objects.lag import PartitionLag
from tests.fakes import SAMPLE_GROUP, FakeConsumerGroupReader


def test_list_groups_sorted_by_id() -> None:
    result = ListConsumerGroupsUseCase(FakeConsumerGroupReader()).execute()
    assert [g.group_id for g in result] == ["billing", "legacy", "orders-service"]


def test_list_groups_filtered_by_state() -> None:
    result = ListConsumerGroupsUseCase(FakeConsumerGroupReader()).execute(
        states=[ConsumerGroupState.STABLE, ConsumerGroupState.EMPTY]
    )
    assert [g.group_id for g in result] == ["billing", "orders-service"]


def test_list_groups_empty_filter_returns_all() -> None:
    assert len(ListConsumerGroupsUseCase(FakeConsumerGroupReader()).execute([])) == 3


def test_describe_group() -> None:
    assert (
        DescribeConsumerGroupUseCase(FakeConsumerGroupReader()).execute("billing")
        == SAMPLE_GROUP
    )


def test_describe_group_validates_id_before_querying() -> None:
    reader = FakeConsumerGroupReader()
    with pytest.raises(InvalidInputError):
        DescribeConsumerGroupUseCase(reader).execute("bad\nid")
    assert reader.requested == []


def test_describe_missing_group() -> None:
    with pytest.raises(KafkaResourceNotFoundError):
        DescribeConsumerGroupUseCase(FakeConsumerGroupReader()).execute("missing")


def test_consumer_lag_is_ordered_and_aggregated() -> None:
    lag = GetConsumerLagUseCase(FakeConsumerGroupReader()).execute("billing")
    assert lag.group_id == "billing"
    assert [p.partition for p in lag.partitions] == [0, 2, 1]
    assert lag.total_lag == 70
    assert lag.unknown_lag_partition_count == 1


def test_consumer_lag_validates_id_and_propagates_errors() -> None:
    with pytest.raises(InvalidInputError):
        GetConsumerLagUseCase(FakeConsumerGroupReader()).execute("")
    reader = FakeConsumerGroupReader(error=KafkaConnectionError("down"))
    with pytest.raises(KafkaConnectionError):
        GetConsumerLagUseCase(reader).execute("billing")


def test_analyze_consumer_health_combines_group_and_lag() -> None:
    reader = FakeConsumerGroupReader()
    report = AnalyzeConsumerHealthUseCase(
        reader, LagThresholds(warning=50, critical=500)
    ).execute("billing")
    assert report.group == SAMPLE_GROUP
    assert report.lag.total_lag == 70
    # payments-0 has lag 60 >= 50; payments-2 has lag but is not assigned to
    # the only member (which holds payments-0 and -1); payments-1 lag is unknown.
    assert report.status is HealthStatus.WARNING
    assert [f.code for f in report.findings] == [
        "LAG_ABOVE_WARNING",
        "UNASSIGNED_PARTITIONS_WITH_LAG",
        "LAG_UNKNOWN",
    ]


def test_analyze_consumer_health_validates_and_propagates_errors() -> None:
    thresholds = LagThresholds(warning=1, critical=2)
    reader = FakeConsumerGroupReader()
    with pytest.raises(InvalidInputError):
        AnalyzeConsumerHealthUseCase(reader, thresholds).execute("")
    assert reader.requested == []
    with pytest.raises(KafkaResourceNotFoundError):
        AnalyzeConsumerHealthUseCase(reader, thresholds).execute("missing")


def test_detect_stalled_partitions_samples_twice_around_the_interval() -> None:
    events: list[str] = []

    class RecordingReader(FakeConsumerGroupReader):
        def get_consumer_lag(self, group_id: str) -> list[PartitionLag]:
            events.append("sample")
            return super().get_consumer_lag(group_id)

    def sleep(seconds: float) -> None:
        events.append(f"sleep {seconds:g}")

    report = DetectStalledPartitionsUseCase(RecordingReader(), sleep=sleep).execute(
        "billing", 7
    )
    assert events == ["sample", "sleep 7", "sample"]
    assert report.interval_seconds == 7
    assert report.group == SAMPLE_GROUP


@pytest.mark.parametrize(("group_id", "interval"), [("", 10), ("billing", 0)])
def test_detect_stalled_partitions_validates_before_sampling(
    group_id: str, interval: float
) -> None:
    reader = FakeConsumerGroupReader()
    use_case = DetectStalledPartitionsUseCase(reader, sleep=lambda _s: None)
    with pytest.raises(InvalidInputError):
        use_case.execute(group_id, interval)
    assert reader.requested == []
