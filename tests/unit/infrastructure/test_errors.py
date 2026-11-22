import pytest
from confluent_kafka import KafkaError, KafkaException

from streamsage.domain.exceptions import (
    KafkaConnectionError,
    KafkaMetadataError,
    KafkaOperationTimeoutError,
    StreamSageError,
)
from streamsage.infrastructure.kafka.errors import (
    error_code_name,
    translate_kafka_exception,
)


@pytest.mark.parametrize(
    "code,expected",
    [
        (KafkaError._TRANSPORT, KafkaConnectionError),
        (KafkaError._ALL_BROKERS_DOWN, KafkaConnectionError),
        (KafkaError._RESOLVE, KafkaConnectionError),
        (KafkaError._AUTHENTICATION, KafkaConnectionError),
        (KafkaError.SASL_AUTHENTICATION_FAILED, KafkaConnectionError),
        (KafkaError._TIMED_OUT, KafkaOperationTimeoutError),
        (KafkaError.REQUEST_TIMED_OUT, KafkaOperationTimeoutError),
        (KafkaError.CLUSTER_AUTHORIZATION_FAILED, KafkaMetadataError),
        (KafkaError.UNKNOWN, KafkaMetadataError),
    ],
)
def test_translation_by_code(code: int, expected: type[StreamSageError]) -> None:
    exc = KafkaException(KafkaError(code, "raw library text with secrets"))
    result = translate_kafka_exception(exc, operation="op", timeout_seconds=3)
    assert type(result) is expected
    assert KafkaError(code).name() in str(result)
    assert "raw library text" not in str(result)


def test_connection_error_mentions_timeout() -> None:
    exc = KafkaException(KafkaError(KafkaError._TRANSPORT))
    result = translate_kafka_exception(
        exc, operation="topic listing", timeout_seconds=2.5
    )
    assert "2.5s" in str(result)
    assert "topic listing" in str(result)


def test_exception_without_kafka_error() -> None:
    result = translate_kafka_exception(
        KafkaException("odd"), operation="op", timeout_seconds=1
    )
    assert isinstance(result, KafkaMetadataError)


def test_error_code_name() -> None:
    assert error_code_name(None) is None
    assert error_code_name(KafkaError(KafkaError.LEADER_NOT_AVAILABLE)) == (
        "LEADER_NOT_AVAILABLE"
    )
