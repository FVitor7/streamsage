import concurrent.futures
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from streamsage.domain.exceptions import (
    KafkaConnectionError,
    KafkaOperationTimeoutError,
)
from streamsage.infrastructure.kafka import gateway as gateway_module
from streamsage.infrastructure.kafka.client_events import KafkaErrorEvents
from streamsage.infrastructure.kafka.gateway import KafkaAdminGateway


class PollingAdmin:
    def __init__(self) -> None:
        self.polls = 0

    def poll(self, timeout: float) -> int:
        self.polls += 1
        return 0


def _gateway(admin: PollingAdmin, timeout: float = 1) -> KafkaAdminGateway:
    return KafkaAdminGateway(
        admin,  # type: ignore[arg-type]
        timeout_seconds=timeout,
        error_events=KafkaErrorEvents(),
    )


def _done(value: Any) -> concurrent.futures.Future[Any]:
    future: concurrent.futures.Future[Any] = concurrent.futures.Future()
    future.set_result(value)
    return future


def test_request_each_returns_value_per_key_and_drains_events() -> None:
    admin = PollingAdmin()
    result = _gateway(admin).request_each(
        "op", lambda _admin, _timeout: {"a": _done(1), "b": _done(2)}
    )
    assert result == {"a": 1, "b": 2}
    assert admin.polls == 1


def test_request_each_times_out_on_a_future_that_never_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gateway_module, "_FUTURE_GRACE_SECONDS", 0.05)
    admin = PollingAdmin()
    with pytest.raises(KafkaOperationTimeoutError, match="op timed out"):
        _gateway(admin, timeout=0.05).request_each(
            "op", lambda _admin, _timeout: {"a": concurrent.futures.Future()}
        )
    assert admin.polls == 1


def test_synchronous_failure_when_starting_a_request_is_translated() -> None:
    def start(_admin: Any, _timeout: float) -> Any:
        raise KafkaException(KafkaError(KafkaError._TRANSPORT))

    for call in (
        _gateway(PollingAdmin()).request,
        _gateway(PollingAdmin()).request_each,
    ):
        with pytest.raises(KafkaConnectionError):
            call("op", start)
