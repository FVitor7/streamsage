"""Message inspection against the seeded local cluster.

customers.updated holds 3 messages with fake personal data and an
Authorization header (see docker/kafka/init-topics.sh).
"""

import time
import uuid

import pytest

from streamsage.application.use_cases.find_messages_by_key import (
    FindMessagesByKeyUseCase,
    KeySearchLimits,
)
from streamsage.application.use_cases.inspect_messages import InspectMessagesUseCase
from streamsage.application.use_cases.list_dlq_topics import ListDlqTopicsUseCase
from streamsage.domain.exceptions import InvalidInputError, KafkaResourceNotFoundError
from streamsage.domain.messages.dlq import DlqTopicMatcher
from streamsage.domain.messages.sanitizer import (
    REDACTED,
    PayloadTreatment,
    SanitizationPolicy,
)
from streamsage.infrastructure.config.settings import Settings
from streamsage.infrastructure.kafka.consumer import (
    INSPECTOR_GROUP_ID,
    KafkaConsumerFactory,
)
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway
from streamsage.infrastructure.kafka.kafka_consumer_group_adapter import (
    KafkaConsumerGroupAdapter,
)
from streamsage.infrastructure.kafka.kafka_message_adapter import KafkaMessageAdapter
from streamsage.infrastructure.kafka.kafka_topic_adapter import KafkaTopicAdapter

pytestmark = pytest.mark.integration


@pytest.fixture
def reader(gateway: KafkaAdminGateway, kafka_settings: Settings) -> KafkaMessageAdapter:
    return KafkaMessageAdapter(
        gateway, KafkaConsumerFactory(kafka_settings), timeout_seconds=10
    )


def _use_case(reader: KafkaMessageAdapter, payloads: bool) -> InspectMessagesUseCase:
    return InspectMessagesUseCase(
        reader, SanitizationPolicy(payloads_enabled=payloads), max_messages=100
    )


def test_latest_messages_without_payloads(reader: KafkaMessageAdapter) -> None:
    result = _use_case(reader, payloads=False).execute("orders.created", limit=5)
    assert result.complete is True
    assert len(result.messages) == 5
    for message in result.messages:
        assert message.key is not None and message.key.startswith("key-")
        assert message.payload_treatment is PayloadTreatment.OMITTED
        assert message.payload_preview is None
        assert message.payload_size
    timestamps = [m.timestamp_ms or 0 for m in result.messages]
    assert timestamps == sorted(timestamps, reverse=True)


def test_sensitive_data_is_sanitized(
    reader: KafkaMessageAdapter, customer_secrets: set[str]
) -> None:
    result = _use_case(reader, payloads=True).execute("customers.updated", limit=10)
    by_key = {m.key: m for m in result.messages}
    assert set(by_key) == {"cust-1", "cust-2", "cust-3"}

    first = by_key["cust-1"]
    assert first.payload_treatment is PayloadTreatment.JSON_REDACTED
    assert first.redacted_fields == ("email", "password", "address.phone")
    headers = {h.key: h for h in first.headers}
    assert headers["Authorization"].value == REDACTED
    assert headers["trace-id"].value == "abc-001"

    assert by_key["cust-2"].redacted_fields == ("api_token", "orders[0].card_number")
    assert by_key["cust-3"].payload_treatment is PayloadTreatment.TEXT_TRUNCATED

    rendered = repr(result)
    assert len(customer_secrets) == 6
    for secret in customer_secrets:
        assert secret not in rendered


def test_read_forward_from_offset(reader: KafkaMessageAdapter) -> None:
    result = _use_case(reader, payloads=False).execute(
        "customers.updated", limit=2, partition=0, start_offset=1
    )
    assert [m.offset for m in result.messages] == [1, 2]


def test_unknown_partition_is_rejected(reader: KafkaMessageAdapter) -> None:
    with pytest.raises(InvalidInputError, match="Partition 7 does not exist"):
        _use_case(reader, payloads=False).execute("customers.updated", 5, partition=7)


def test_missing_topic_is_not_found_and_never_created(
    reader: KafkaMessageAdapter, gateway: KafkaAdminGateway
) -> None:
    name = f"streamsage.it.inspect.{uuid.uuid4().hex}"
    with pytest.raises(KafkaResourceNotFoundError):
        _use_case(reader, payloads=False).execute(name, limit=1)
    time.sleep(3)
    assert name not in {t.name for t in KafkaTopicAdapter(gateway).list_topics()}


def test_inspection_leaves_no_consumer_group(
    reader: KafkaMessageAdapter, gateway: KafkaAdminGateway
) -> None:
    _use_case(reader, payloads=False).execute("orders.created", limit=3)
    groups = {
        g.group_id for g in KafkaConsumerGroupAdapter(gateway).list_consumer_groups()
    }
    assert INSPECTOR_GROUP_ID not in groups


def _key_search(
    reader: KafkaMessageAdapter, per_partition: int = 1_000, total: int = 10_000
) -> FindMessagesByKeyUseCase:
    return FindMessagesByKeyUseCase(
        reader,
        SanitizationPolicy(),
        KeySearchLimits(
            max_results=20, max_scan_per_partition=per_partition, max_scan_total=total
        ),
    )


def test_find_messages_by_key_over_the_whole_topic(reader: KafkaMessageAdapter) -> None:
    result = _key_search(reader).execute("orders.created", "key-288", limit=5)
    assert [m.key for m in result.matches] == ["key-288"]
    assert result.scanned == 300
    assert result.searched_whole_topic is True
    assert result.notes == ()


def test_key_not_found_with_full_coverage(reader: KafkaMessageAdapter) -> None:
    result = _key_search(reader).execute("orders.created", "no-such-key", limit=5)
    assert result.matches == ()
    assert result.searched_whole_topic is True


def test_key_search_budget_limits_coverage(reader: KafkaMessageAdapter) -> None:
    result = _key_search(reader, total=30).execute("orders.created", "key-1", limit=5)
    assert result.per_partition_limit == 10
    assert result.scanned == 30
    assert result.searched_whole_topic is False
    assert "No match does not mean the key is absent" in result.notes[0]


def test_list_dlq_topics(
    reader: KafkaMessageAdapter, gateway: KafkaAdminGateway
) -> None:
    overview = ListDlqTopicsUseCase(
        KafkaTopicAdapter(gateway), reader, DlqTopicMatcher(["*.DLQ"])
    ).execute()
    assert overview.configured is True
    assert [(t.name, t.message_count) for t in overview.topics] == [
        ("orders.created.DLQ", 2)
    ]


def test_dlq_messages_expose_error_headers(reader: KafkaMessageAdapter) -> None:
    result = _use_case(reader, payloads=False).execute("orders.created.DLQ", limit=5)
    headers = {h.key: h.value for m in result.messages for h in m.headers}
    assert headers["error.class"] in {
        "InvalidOrderException",
        "DeserializationException",
    }
    assert "original.offset" in headers
