"""Single entry point for requests to the Kafka AdminClient.

Every adapter goes through :class:`KafkaAdminGateway`, which applies the same
rules to every request:

- an explicit timeout;
- draining of librdkafka's queued logs and error events after each request;
- translation of library errors into StreamSage errors, using the error
  events to explain generic transport failures (authentication, TLS).
"""

import concurrent.futures
import logging
from collections.abc import Callable, Hashable, Mapping
from typing import TypeVar

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin._metadata import ClusterMetadata

from streamsage.domain.exceptions import (
    KafkaConnectionError,
    KafkaOperationTimeoutError,
    KafkaResourceNotFoundError,
    StreamSageError,
)
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.errors import translate_kafka_exception

logger = logging.getLogger(__name__)

T = TypeVar("T")
K = TypeVar("K", bound=Hashable)

# Error codes meaning "the requested resource does not exist".
NOT_FOUND_CODES = frozenset(
    {KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.GROUP_ID_NOT_FOUND}
)
# librdkafka enforces `request_timeout` itself; this extra margin only guards
# against a future that never completes.
_FUTURE_GRACE_SECONDS = 5.0

RequestStarter = Callable[[AdminClient, float], concurrent.futures.Future[T]]
MultiRequestStarter = Callable[
    [AdminClient, float], Mapping[K, concurrent.futures.Future[T]]
]


class KafkaAdminGateway:
    """Runs admin requests with a bounded timeout and normalized errors.

    ``error_events`` must be the collector registered as the client's
    ``error_cb`` (see ``KafkaAdminClientFactory``).
    """

    def __init__(
        self,
        admin_client: AdminClient,
        timeout_seconds: float,
        error_events: KafkaErrorEvents,
    ) -> None:
        self._admin_client = admin_client
        self._timeout_seconds = timeout_seconds
        self._error_events = error_events

    def cluster_metadata(self, operation: str) -> ClusterMetadata:
        """Full cluster metadata: brokers, controller and *all* topics.

        Metadata is never requested for a specific topic name: on brokers
        with ``auto.create.topics.enable=true`` that can create the topic.
        """
        try:
            metadata = self._admin_client.list_topics(timeout=self._timeout_seconds)
        except KafkaException as exc:
            raise self._failure(exc, operation) from exc
        self._after_success()
        return metadata

    def request(
        self,
        operation: str,
        start: RequestStarter[T],
        *,
        not_found_message: str | None = None,
    ) -> T:
        """Start a future-based admin request and wait for its result.

        ``start`` receives the client and the timeout in seconds and returns
        the future to wait for. If Kafka reports that the resource does not
        exist, ``KafkaResourceNotFoundError(not_found_message)`` is raised.
        """
        try:
            future = start(self._admin_client, self._timeout_seconds)
            result = future.result(
                timeout=self._timeout_seconds + _FUTURE_GRACE_SECONDS
            )
        except KafkaException as exc:
            if not_found_message is not None and _is_not_found(exc):
                self._after_success()
                raise KafkaResourceNotFoundError(not_found_message) from exc
            raise self._failure(exc, operation) from exc
        except concurrent.futures.TimeoutError as exc:
            self._drain()
            raise KafkaOperationTimeoutError(
                f"Kafka {operation} timed out after {self._timeout_seconds:g}s."
            ) from exc
        self._after_success()
        return result

    def request_each(
        self, operation: str, start: MultiRequestStarter[K, T]
    ) -> dict[K, T | None]:
        """Run an admin request that returns one future per key.

        A failure that only concerns one key (for example an unknown
        partition) yields ``None`` for that key. A failure of the request as
        a whole (connection, authentication, TLS or timeout) is raised, since
        partial results would be misleading.
        """
        deadline_seconds = self._timeout_seconds + _FUTURE_GRACE_SECONDS
        results: dict[K, T | None] = {}
        try:
            futures = start(self._admin_client, self._timeout_seconds)
            for key, future in futures.items():
                try:
                    results[key] = future.result(timeout=deadline_seconds)
                except KafkaException as exc:
                    failure = translate_kafka_exception(
                        exc, operation=operation, timeout_seconds=self._timeout_seconds
                    )
                    if isinstance(
                        failure, KafkaConnectionError | KafkaOperationTimeoutError
                    ):
                        raise
                    results[key] = None
        except KafkaException as exc:
            raise self._failure(exc, operation) from exc
        except concurrent.futures.TimeoutError as exc:
            self._drain()
            raise KafkaOperationTimeoutError(
                f"Kafka {operation} timed out after {self._timeout_seconds:g}s."
            ) from exc
        self._after_success()
        return results

    def _failure(self, exc: KafkaException, operation: str) -> StreamSageError:
        self._drain()
        translated = translate_kafka_exception(
            exc,
            operation=operation,
            timeout_seconds=self._timeout_seconds,
            observed_codes=self._error_events.take_codes(),
        )
        logger.warning(
            "kafka request failed",
            extra={
                "event": {
                    "operation": operation,
                    "error_type": type(translated).__name__,
                    "kafka_error": str(exc.args[0]) if exc.args else None,
                }
            },
        )
        return translated

    def _after_success(self) -> None:
        self._drain()
        self._error_events.take_codes()  # discard events from before this success

    def _drain(self) -> None:
        # librdkafka queues logs and error events until the application polls.
        # Polling after every request delivers them (logs reach Python logging,
        # errors reach KafkaErrorEvents) and keeps the queue from growing while
        # a broker is unreachable.
        self._admin_client.poll(0)


def _is_not_found(exc: KafkaException) -> bool:
    error = exc.args[0] if exc.args else None
    return isinstance(error, KafkaError) and error.code() in NOT_FOUND_CODES
