"""Consumer groups and lag against the seeded local cluster.

docker/kafka/init-topics.sh seeds deterministic data:
  orders-service    committed offset 40 on each of the 3 partitions of
                    orders.created, which holds 300 messages -> total lag 180
  payments-service  fully caught up on payments.processed -> lag 0
  inventory-service consumed by the `kafka-consumer` service (active group)
"""

import time

import pytest
from confluent_kafka import Producer

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
from streamsage.domain.diagnostics.consumer_progress import ProgressStatus
from streamsage.domain.diagnostics.severity import HealthStatus
from streamsage.domain.entities.consumer_group import ConsumerGroup, ConsumerGroupState
from streamsage.domain.exceptions import KafkaResourceNotFoundError
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway
from streamsage.infrastructure.kafka.kafka_consumer_group_adapter import (
    KafkaConsumerGroupAdapter,
)

pytestmark = pytest.mark.integration

ORDERS_MESSAGES = 300
ORDERS_COMMITTED = 40
ORDERS_PARTITIONS = 3


@pytest.fixture
def adapter(gateway: KafkaAdminGateway) -> KafkaConsumerGroupAdapter:
    return KafkaConsumerGroupAdapter(gateway)


def test_seeded_groups_are_listed(adapter: KafkaConsumerGroupAdapter) -> None:
    groups = {g.group_id: g for g in ListConsumerGroupsUseCase(adapter).execute()}
    assert {"orders-service", "payments-service", "inventory-service"} <= set(groups)
    assert groups["orders-service"].state is ConsumerGroupState.EMPTY
    assert groups["orders-service"].is_simple is True


def test_lag_matches_seeded_offsets(adapter: KafkaConsumerGroupAdapter) -> None:
    lag = GetConsumerLagUseCase(adapter).execute("orders-service")

    assert len(lag.partitions) == ORDERS_PARTITIONS
    for partition in lag.partitions:
        assert partition.topic == "orders.created"
        assert partition.committed_offset == ORDERS_COMMITTED
        assert partition.log_end_offset is not None
        assert partition.lag == partition.log_end_offset - ORDERS_COMMITTED
    assert lag.total_lag == ORDERS_MESSAGES - ORDERS_COMMITTED * ORDERS_PARTITIONS
    assert lag.unknown_lag_partition_count == 0
    lags = [p.lag for p in lag.partitions]
    assert lags == sorted(lags, reverse=True)


def test_caught_up_group_has_zero_lag(adapter: KafkaConsumerGroupAdapter) -> None:
    lag = GetConsumerLagUseCase(adapter).execute("payments-service")
    assert lag.total_lag == 0
    assert all(p.lag == 0 for p in lag.partitions)


def test_active_group_has_member_and_assignment(
    adapter: KafkaConsumerGroupAdapter,
) -> None:
    group = _wait_for_stable_group(adapter, "inventory-service")
    assert group.member_count == 1
    assert group.partition_assignor
    assert group.coordinator_id is not None
    assert [(tp.topic, tp.partition) for tp in group.assigned_partitions] == [
        ("inventory.updated", 0)
    ]


def test_missing_group_is_not_found(adapter: KafkaConsumerGroupAdapter) -> None:
    for call in (
        DescribeConsumerGroupUseCase(adapter).execute,
        GetConsumerLagUseCase(adapter).execute,
    ):
        with pytest.raises(KafkaResourceNotFoundError):
            call("streamsage-it-no-such-group")


def _wait_for_stable_group(
    adapter: KafkaConsumerGroupAdapter, group_id: str, timeout: float = 60
) -> ConsumerGroup:
    deadline = time.monotonic() + timeout
    group = None
    while time.monotonic() < deadline:
        try:
            group = DescribeConsumerGroupUseCase(adapter).execute(group_id)
            # A freshly started consumer joins before its first commit; until
            # then its lag is unknown. Wait for both.
            committed = all(
                p.committed_offset is not None
                for p in adapter.get_consumer_lag(group_id)
            )
            if group.state is ConsumerGroupState.STABLE and group.members and committed:
                return group
        except KafkaResourceNotFoundError:
            pass
        time.sleep(1)
    pytest.fail(
        f"{group_id} not stable with committed offsets after {timeout}s "
        f"(last: {group}). Start it with `docker compose up -d kafka-consumer`."
    )


DEFAULT_THRESHOLDS = LagThresholds(warning=1_000, critical=10_000)


def test_health_of_inactive_group_with_pending_messages(
    adapter: KafkaConsumerGroupAdapter,
) -> None:
    report = AnalyzeConsumerHealthUseCase(adapter, DEFAULT_THRESHOLDS).execute(
        "orders-service"
    )
    # Max partition lag (~66) is far below the thresholds, but nobody consumes.
    assert report.status is HealthStatus.WARNING
    assert [f.code for f in report.findings] == ["NO_ACTIVE_MEMBERS"]


@pytest.mark.parametrize("group_id", ["payments-service", "inventory-service"])
def test_health_of_caught_up_groups(
    adapter: KafkaConsumerGroupAdapter, group_id: str
) -> None:
    if group_id == "inventory-service":
        _wait_for_stable_group(adapter, group_id)
    report = AnalyzeConsumerHealthUseCase(adapter, DEFAULT_THRESHOLDS).execute(group_id)
    assert report.status is HealthStatus.HEALTHY
    assert report.lag.total_lag == 0


def test_inactive_group_partitions_are_not_advancing(
    adapter: KafkaConsumerGroupAdapter,
) -> None:
    report = DetectStalledPartitionsUseCase(adapter).execute("orders-service", 2)
    assert {p.status for p in report.partitions} == {ProgressStatus.NOT_ADVANCING}
    assert report.count(ProgressStatus.NOT_ADVANCING) == ORDERS_PARTITIONS
    assert report.status is HealthStatus.WARNING
    assert [f.code for f in report.findings] == ["PARTITIONS_NOT_ADVANCING"]


def test_active_consumer_is_advancing(
    adapter: KafkaConsumerGroupAdapter, kafka_settings: Settings
) -> None:
    # Test-only writes: produce while sampling so the active sample consumer
    # (auto-commit every 5s) has something to consume and commit.
    _wait_for_stable_group(adapter, "inventory-service")
    use_case = DetectStalledPartitionsUseCase(
        adapter, sleep=lambda seconds: _produce_then_wait(kafka_settings, seconds)
    )
    report = use_case.execute("inventory-service", 8)
    (partition,) = report.partitions
    assert partition.status is ProgressStatus.ADVANCING
    assert (partition.committed_delta or 0) > 0
    assert report.status is HealthStatus.HEALTHY


def _produce_then_wait(settings: Settings, seconds: float) -> None:
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for i in range(10):
        producer.produce("inventory.updated", key=f"it-{i}", value=b"{}")
    producer.flush(5)
    time.sleep(seconds)
