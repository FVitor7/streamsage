"""Translation of confluent-kafka errors into StreamSage errors.

Only Kafka error *codes* are surfaced in messages; raw library text is kept
out of client-facing errors.
"""

from collections.abc import Collection

from confluent_kafka import KafkaError, KafkaException

from streamsage.domain.exceptions import (
    KafkaConnectionError,
    KafkaMetadataError,
    KafkaOperationTimeoutError,
    StreamSageError,
)

_CONNECTION_CODES = frozenset(
    {
        KafkaError._TRANSPORT,
        KafkaError._ALL_BROKERS_DOWN,
        KafkaError._RESOLVE,
    }
)
_AUTHENTICATION_CODES = frozenset(
    {
        KafkaError._AUTHENTICATION,
        KafkaError.SASL_AUTHENTICATION_FAILED,
    }
)
_TLS_CODES = frozenset({KafkaError._SSL})
_TIMEOUT_CODES = frozenset(
    {
        KafkaError._TIMED_OUT,
        KafkaError._TIMED_OUT_QUEUE,
        KafkaError.REQUEST_TIMED_OUT,
    }
)


def error_code_name(error: KafkaError | None) -> str | None:
    """Return the symbolic name of a Kafka error code, if any."""
    return None if error is None else error.name()


def translate_kafka_exception(
    exc: KafkaException,
    *,
    operation: str,
    timeout_seconds: float,
    observed_codes: Collection[int] = (),
) -> StreamSageError:
    """Map a ``KafkaException`` to the matching StreamSage error.

    ``observed_codes`` are error codes the client reported through its error
    callback around the failed request. librdkafka surfaces authentication
    and TLS failures there while the request itself only fails with a
    generic transport error, so they take precedence when present.
    """
    error = exc.args[0] if exc.args and isinstance(exc.args[0], KafkaError) else None
    if error is None:
        return KafkaMetadataError(f"Kafka {operation} failed.")

    codes = {error.code(), *observed_codes}
    if codes & _AUTHENTICATION_CODES:
        return KafkaConnectionError(
            f"Authentication with the Kafka cluster failed during {operation}. "
            "Check the configured SASL mechanism, username and password."
        )
    if codes & _TLS_CODES:
        return KafkaConnectionError(
            f"TLS handshake with the Kafka cluster failed during {operation}. "
            "Check that the broker certificate is trusted "
            "(STREAMSAGE_KAFKA_SSL_CA_LOCATION) and matches the broker host name."
        )

    code = error.code()
    name = error.name()
    if code in _CONNECTION_CODES:
        return KafkaConnectionError(
            f"Could not connect to the Kafka cluster during {operation} "
            f"(waited up to {timeout_seconds:g}s; code {name})."
        )
    if code in _TIMEOUT_CODES:
        return KafkaOperationTimeoutError(
            f"Kafka {operation} timed out after {timeout_seconds:g}s (code {name})."
        )
    return KafkaMetadataError(f"Kafka {operation} failed (code {name}).")
