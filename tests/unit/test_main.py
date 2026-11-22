from contextlib import AbstractContextManager
from typing import Any

import pytest

from streamsage import main as main_module
from streamsage.domain.exceptions import ConfigurationError
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
        main_module.KafkaAdminClientFactory, "create", lambda self, events: admin
    )
    with main_module.build_application(Settings()) as app:
        assert app.server.name == "streamsage"
        assert app.metrics.registry is not None
        assert admin.closed is False
    assert admin.closed is True


def test_main_exits_cleanly_when_kafka_rejects_configuration(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def reject(self: object, events: object) -> None:
        raise ConfigurationError("The Kafka client rejected the configuration.")

    monkeypatch.setattr(main_module.KafkaAdminClientFactory, "create", reject)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)
    with pytest.raises(SystemExit) as exit_info:
        main_module.main([])

    assert exit_info.value.code == main_module.EXIT_CONFIG_ERROR
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "rejected the configuration" in captured.err
    assert "Traceback" not in captured.err


def test_unparseable_environment_value_exits_without_leaking_it(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    value = "not-json-" + "x" * 8
    monkeypatch.setenv("STREAMSAGE_FINDING_SEVERITY_OVERRIDES", value)
    with pytest.raises(SystemExit) as exit_info:
        main_module.load_settings()
    assert exit_info.value.code == main_module.EXIT_CONFIG_ERROR
    err = capsys.readouterr().err
    assert "finding_severity_overrides" in err
    assert value not in err


def test_generate_token_prints_token_and_matching_entry(
    capsys: pytest.CaptureFixture[str],
) -> None:
    import json

    from streamsage.presentation.http.bearer_auth import hash_token

    main_module.main(
        [
            "generate-token",
            "--name",
            "ci",
            "--scope",
            "kafka:messages",
            "--scope",
            "kafka:metadata",
        ]
    )
    lines = [
        line.strip() for line in capsys.readouterr().out.splitlines() if line.strip()
    ]
    token = lines[1]
    entry = json.loads(lines[-1])
    assert entry == {
        "name": "ci",
        "sha256": hash_token(token),
        "scopes": ["kafka:messages", "kafka:metadata"],
    }


def test_generate_token_defaults_to_metadata_scope(
    capsys: pytest.CaptureFixture[str],
) -> None:
    main_module.main(["generate-token", "--name", "ci"])
    assert '"scopes": ["kafka:metadata"]' in capsys.readouterr().out


def test_generate_token_rejects_unknown_scope() -> None:
    with pytest.raises(SystemExit):
        main_module.main(["generate-token", "--name", "ci", "--scope", "kafka:admin"])
