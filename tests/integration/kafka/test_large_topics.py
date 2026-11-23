"""Resource limits against a real broker: many partitions and huge messages.

Test-only setup creates two topics and produces to them; StreamSage itself
only reads.
"""

import time
from collections.abc import Iterator

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from streamsage.application.use_cases.find_messages_by_key import (
    FindMessagesByKeyUseCase,
    KeySearchLimits,
)
from streamsage.application.use_cases.inspect_messages import InspectMessagesUseCase
from streamsage.domain.messages.sanitizer import PayloadTreatment, SanitizationPolicy
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.consumer import KafkaConsumerFactory
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway
from streamsage.infrastructure.kafka.kafka_message_adapter import KafkaMessageAdapter

pytestmark = pytest.mark.integration

MANY = "streamsage.it.many-partitions"
LARGE = "streamsage.it.large-messages"
PARTITIONS = 1_000
LARGE_MESSAGE_BYTES = 5_000_000


@pytest.fixture(scope="module")
def large_topics(admin_client: AdminClient, kafka_settings: Settings) -> Iterator[None]:
    existing = admin_client.list_topics(timeout=10).topics
    new = [
        NewTopic(MANY, PARTITIONS, 1),
        NewTopic(LARGE, 1, 1, config={"max.message.bytes": str(6 * 1024 * 1024)}),
    ]
    to_create = [t for t in new if t.topic not in existing]
    if to_create:
        for future in admin_client.create_topics(to_create).values():
            future.result()
        time.sleep(3)  # let partition leadership settle
        producer = Producer(
            {
                "bootstrap.servers": kafka_settings.kafka_bootstrap_servers,
                "message.max.bytes": 6 * 1024 * 1024,
            }
        )
        for i in range(PARTITIONS * 3):
            producer.produce(MANY, key=f"k-{i}", value=b"{}", partition=i % PARTITIONS)
        producer.produce(
            LARGE, key="big", value=b'{"blob":"' + b"x" * LARGE_MESSAGE_BYTES + b'"}'
        )
        producer.produce(LARGE, key="small", value=b'{"ok":true}')
        producer.flush(30)
    yield


@pytest.fixture
def reader(gateway: KafkaAdminGateway, kafka_settings: Settings) -> KafkaMessageAdapter:
    return KafkaMessageAdapter(
        gateway, KafkaConsumerFactory(kafka_settings), timeout_seconds=10
    )


@pytest.mark.usefixtures("large_topics")
def test_offsets_of_a_thousand_partitions_in_one_batch(
    reader: KafkaMessageAdapter,
) -> None:
    started = time.monotonic()
    offsets = reader.partition_offsets(MANY)
    assert offsets.partition_count == PARTITIONS
    assert offsets.unavailable_partitions == ()
    assert all(p.message_count >= 3 for p in offsets.partitions)
    assert time.monotonic() - started < 5


@pytest.mark.usefixtures("large_topics")
def test_inspection_respects_the_global_read_budget(
    reader: KafkaMessageAdapter,
) -> None:
    use_case = InspectMessagesUseCase(
        reader, SanitizationPolicy(), max_messages=100, max_read=1_000
    )
    result = use_case.execute(MANY, limit=100)
    assert result.messages_read <= 1_000
    assert len(result.messages) == 100
    assert result.exact is False
    assert any("Read budget reached" in note for note in result.notes)


@pytest.mark.usefixtures("large_topics")
def test_inspection_is_exact_when_the_budget_allows(
    reader: KafkaMessageAdapter,
) -> None:
    use_case = InspectMessagesUseCase(
        reader, SanitizationPolicy(), max_messages=100, max_read=10_000
    )
    result = use_case.execute(MANY, limit=3)
    assert result.exact is True
    assert result.messages_read == 3 * PARTITIONS


@pytest.mark.usefixtures("large_topics")
def test_huge_messages_are_reported_but_never_decoded(
    reader: KafkaMessageAdapter,
) -> None:
    use_case = InspectMessagesUseCase(
        reader, SanitizationPolicy(payloads_enabled=True), max_messages=100
    )
    result = use_case.execute(LARGE, limit=10)
    by_key = {m.key: m for m in result.messages}
    assert by_key["big"].payload_treatment is PayloadTreatment.TOO_LARGE
    assert by_key["big"].payload_size == LARGE_MESSAGE_BYTES + len('{"blob":""}')
    assert by_key["small"].payload_treatment is PayloadTreatment.JSON_REDACTED


@pytest.mark.usefixtures("large_topics")
def test_key_search_budget_is_a_hard_limit_on_a_thousand_partitions(
    reader: KafkaMessageAdapter,
) -> None:
    use_case = FindMessagesByKeyUseCase(
        reader,
        SanitizationPolicy(),
        KeySearchLimits(
            max_results=5, max_scan_per_partition=1_000, max_scan_total=100
        ),
    )
    # Message i went to partition i % 1000, so the newest in partition 5 is
    # "k-2005"; only the first 100 partitions fit in the budget.
    result = use_case.execute(MANY, "k-2005", limit=5)
    assert result.scanned <= 100
    assert [(m.partition, m.key) for m in result.matches] == [(5, "k-2005")]
    assert result.uncovered_partitions == tuple(range(100, PARTITIONS))
    assert result.searched_whole_topic is False
