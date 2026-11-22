"""Message inspection against the seeded local cluster.

customers.updated holds 3 messages with fake personal data and an
Authorization header (see docker/kafka/init-topics.sh).
"""

import time
import uuid

import pytest

from streamsage.application.use_cases.inspect_messages import InspectMessagesUseCase
from streamsage.domain.exceptions import InvalidInputError, KafkaResourceNotFoundError
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
