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


def _translate(code: int, observed: tuple[int, ...] = ()) -> StreamSageError:
    exc = KafkaException(KafkaError(code, "raw library text with secrets"))
    return translate_kafka_exception(
        exc, operation="op", timeout_seconds=3, observed_codes=observed
    )


@pytest.mark.parametrize(
    "code,expected",
    [
        (KafkaError._TRANSPORT, KafkaConnectionError),
        (KafkaError._ALL_BROKERS_DOWN, KafkaConnectionError),
        (KafkaError._RESOLVE, KafkaConnectionError),
        (KafkaError._TIMED_OUT, KafkaOperationTimeoutError),
        (KafkaError.REQUEST_TIMED_OUT, KafkaOperationTimeoutError),
        (KafkaError.CLUSTER_AUTHORIZATION_FAILED, KafkaMetadataError),
        (KafkaError.UNKNOWN, KafkaMetadataError),
    ],
)
def test_translation_by_code(code: int, expected: type[StreamSageError]) -> None:
    result = _translate(code)
    assert type(result) is expected
    assert KafkaError(code).name() in str(result)
    assert "raw library text" not in str(result)


@pytest.mark.parametrize(
    "code", [KafkaError._AUTHENTICATION, KafkaError.SASL_AUTHENTICATION_FAILED]
)
def test_authentication_failures(code: int) -> None:
    result = _translate(code)
    assert isinstance(result, KafkaConnectionError)
    assert "Authentication with the Kafka cluster failed" in str(result)
    assert "raw library text" not in str(result)


def test_tls_failure() -> None:
    result = _translate(KafkaError._SSL)
    assert isinstance(result, KafkaConnectionError)
    assert "TLS handshake" in str(result)
    assert "STREAMSAGE_KAFKA_SSL_CA_LOCATION" in str(result)


@pytest.mark.parametrize(
    "observed,expected_text",
    [
        ((KafkaError._AUTHENTICATION,), "Authentication with the Kafka cluster"),
        ((KafkaError._SSL,), "TLS handshake"),
        ((KafkaError._SSL, KafkaError._AUTHENTICATION), "Authentication"),
        ((KafkaError._ALL_BROKERS_DOWN,), "Could not connect"),
        ((), "Could not connect"),
    ],
)
def test_observed_codes_explain_generic_transport_errors(
    observed: tuple[int, ...], expected_text: str
) -> None:
    # librdkafka fails the request with _TRANSPORT and reports the real cause
    # only through error_cb.
    result = _translate(KafkaError._TRANSPORT, observed)
    assert isinstance(result, KafkaConnectionError)
    assert expected_text in str(result)


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
