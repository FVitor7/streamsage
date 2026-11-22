"""Collection of librdkafka error events.

librdkafka reports *why* a broker connection failed (for example a SASL
authentication failure or a TLS handshake failure) only through the client's
``error_cb``, while the failed request itself just reports a generic
transport error. This collector keeps the error codes seen between two
requests so the adapters can explain failures precisely.

Only integer error codes are stored, never the error text.
"""

import threading

from confluent_kafka import KafkaError


class KafkaErrorEvents:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._codes: set[int] = set()

    def record(self, error: KafkaError) -> None:
        """``error_cb`` for the Kafka client. Runs on the polling thread."""
        with self._lock:
            self._codes.add(error.code())

    def take_codes(self) -> frozenset[int]:
        """Return the codes recorded since the previous call and reset."""
        with self._lock:
            codes = frozenset(self._codes)
            self._codes.clear()
        return codes
