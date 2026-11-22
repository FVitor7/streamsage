"""Translation of confluent-kafka errors into StreamSage errors.

Only Kafka error *codes* are surfaced in messages; raw library text is kept
out of client-facing errors.
"""

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
        KafkaError._SSL,
    }
)
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
    exc: KafkaException, *, operation: str, timeout_seconds: float
) -> StreamSageError:
    """Map a ``KafkaException`` to the matching StreamSage error."""
    error = exc.args[0] if exc.args and isinstance(exc.args[0], KafkaError) else None
    if error is None:
        return KafkaMetadataError(f"Kafka {operation} failed.")

    code = error.code()
    name = error.name()
    if code in _CONNECTION_CODES:
        return KafkaConnectionError(
            f"Could not connect to the Kafka cluster during {operation} "
            f"(waited up to {timeout_seconds:g}s; code {name})."
        )
    if code in _AUTHENTICATION_CODES:
        return KafkaConnectionError(
            f"Authentication with the Kafka cluster failed during {operation} "
            f"(code {name}). Check the configured credentials and TLS settings."
        )
    if code in _TIMEOUT_CODES:
        return KafkaOperationTimeoutError(
            f"Kafka {operation} timed out after {timeout_seconds:g}s (code {name})."
        )
    return KafkaMetadataError(f"Kafka {operation} failed (code {name}).")
