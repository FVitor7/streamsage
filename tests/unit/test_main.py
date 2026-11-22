from contextlib import AbstractContextManager
from typing import Any

import pytest

from streamsage import main as main_module
from streamsage.infrastructure.config.settings import Settings
from tests.fakes import random_credential


def test_invalid_configuration_exits_without_leaking_values(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("STREAMSAGE_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    password = random_credential()
    monkeypatch.setenv("STREAMSAGE_KAFKA_SASL_PASSWORD", password)
    monkeypatch.setenv("STREAMSAGE_KAFKA_REQUEST_TIMEOUT", "-5")

    with pytest.raises(SystemExit) as exit_info:
        main_module.load_settings()

    assert exit_info.value.code == main_module.EXIT_CONFIG_ERROR
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "invalid configuration" in captured.err
    assert "kafka_request_timeout" in captured.err
    assert password not in captured.err


class _FakeAdminClient(AbstractContextManager["_FakeAdminClient"]):
    def __init__(self) -> None:
        self.closed = False

    def __exit__(self, *exc: Any) -> None:
        self.closed = True


def test_build_application_wires_server_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin = _FakeAdminClient()
    monkeypatch.setattr(
        main_module.KafkaAdminClientFactory, "create", lambda self: admin
    )
    with main_module.build_application(Settings()) as server:
        assert server.name == "streamsage"
        assert admin.closed is False
    assert admin.closed is True
