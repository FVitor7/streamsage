import json

import pytest

from streamsage.domain.entities.message import RawHeader, RawMessage, TimestampType
from streamsage.domain.exceptions import InvalidInputError
from streamsage.domain.messages.sanitizer import (
    MAX_REPORTED_REDACTIONS,
    REDACTED,
    ContentEncoding,
    PayloadTreatment,
    SanitizationPolicy,
    sanitize_message,
    truncate_utf8,
)
from tests.fakes import random_credential

ENABLED = SanitizationPolicy(payloads_enabled=True)


def _message(
    value: bytes | None = b"{}",
    key: bytes | None = b"k-1",
    headers: tuple[RawHeader, ...] = (),
) -> RawMessage:
    return RawMessage(
        topic="t",
        partition=0,
        offset=7,
        timestamp_ms=1_700_000_000_000,
        timestamp_type=TimestampType.CREATE_TIME,
        key=key,
        value=value,
        headers=headers,
    )


def test_payloads_are_omitted_by_default_but_size_is_reported() -> None:
    raw = json.dumps({"password": random_credential()}).encode()
    result = sanitize_message(_message(raw), SanitizationPolicy())
    assert result.payload_treatment is PayloadTreatment.OMITTED
    assert result.payload_preview is None
    assert result.payload_size == len(raw)
    assert result.redacted_fields == ()


def test_metadata_is_preserved() -> None:
    result = sanitize_message(_message(), SanitizationPolicy())
    assert (result.topic, result.partition, result.offset) == ("t", 0, 7)
    assert result.timestamp_ms == 1_700_000_000_000
    assert result.timestamp_type is TimestampType.CREATE_TIME
    assert (result.key, result.key_size, result.key_encoding) == (
        "k-1",
        3,
        ContentEncoding.TEXT,
    )


def test_null_payload_is_reported_as_null() -> None:
    result = sanitize_message(_message(value=None), ENABLED)
    assert result.payload_treatment is PayloadTreatment.NULL
    assert result.payload_size is None


def test_sensitive_json_fields_are_redacted_recursively() -> None:
    document = {
        "id": 1,
        "userPassword": random_credential(),
        "profile": {"Email": "a@b.c", "city": "Recife"},
        "cards": [{"card_number": "0000", "brand": "x"}, {"cvv": "123"}],
        "api-key": "k",
    }
    result = sanitize_message(_message(json.dumps(document).encode()), ENABLED)
    assert result.payload_treatment is PayloadTreatment.JSON_REDACTED
    assert result.payload_preview is not None
    preview = json.loads(result.payload_preview)
    assert preview == {
        "id": 1,
        "userPassword": REDACTED,
        "profile": {"Email": REDACTED, "city": "Recife"},
        "cards": [{"card_number": REDACTED, "brand": "x"}, {"cvv": REDACTED}],
        "api-key": REDACTED,
    }
    assert result.redacted_fields == (
        "userPassword",
        "profile.Email",
        "cards[0].card_number",
        "cards[1].cvv",
        "api-key",
    )


def test_a_sensitive_object_is_redacted_as_a_whole() -> None:
    nested = {"user": "u", "key": random_credential()}
    raw = json.dumps({"credentials": nested}).encode()
    result = sanitize_message(_message(raw), ENABLED)
    assert result.payload_preview == '{"credentials":"[REDACTED]"}'


def test_sensitive_values_never_survive_truncation() -> None:
    secret = random_credential()
    raw = json.dumps({"token": secret, "data": "x" * 5000}).encode()
    result = sanitize_message(
        _message(raw), SanitizationPolicy(payloads_enabled=True, max_payload_bytes=40)
    )
    assert result.payload_truncated is True
    assert result.payload_preview is not None
    assert len(result.payload_preview.encode()) <= 40
    assert secret not in result.payload_preview


def test_extra_sensitive_terms_are_configurable() -> None:
    policy = SanitizationPolicy(
        payloads_enabled=True, sensitive_terms=frozenset({"Birth_Date"})
    )
    result = sanitize_message(_message(b'{"birthDate":"2000","email":"a"}'), policy)
    # Only the configured terms apply when replacing the defaults.
    assert result.payload_preview == '{"birthDate":"[REDACTED]","email":"a"}'


def test_redaction_report_is_capped() -> None:
    document = {f"token{i}": i for i in range(MAX_REPORTED_REDACTIONS + 5)}
    result = sanitize_message(_message(json.dumps(document).encode()), ENABLED)
    assert len(result.redacted_fields) == MAX_REPORTED_REDACTIONS


def test_non_json_text_is_only_truncated() -> None:
    result = sanitize_message(
        _message(b"hello world"),
        SanitizationPolicy(payloads_enabled=True, max_payload_bytes=5),
    )
    assert result.payload_treatment is PayloadTreatment.TEXT_TRUNCATED
    assert (result.payload_preview, result.payload_truncated) == ("hello", True)


@pytest.mark.parametrize("raw", [b"\x00\x00\x00\x00\x01\xff\xfe", b"\xc3\x28"])
def test_binary_payloads_are_never_decoded(raw: bytes) -> None:
    result = sanitize_message(_message(raw), ENABLED)
    assert result.payload_treatment is PayloadTreatment.BINARY
    assert result.payload_preview is None
    assert result.payload_size == len(raw)


def test_sensitive_header_values_are_withheld() -> None:
    headers = (
        RawHeader("Authorization", random_credential().encode()),
        RawHeader("x-api-key", random_credential().encode()),
        RawHeader("trace-id", b"t-1"),
        RawHeader("empty", None),
        RawHeader("bin", b"\xff\xfe"),
    )
    result = sanitize_message(_message(headers=headers), SanitizationPolicy())
    by_key = {h.key: h for h in result.headers}
    assert by_key["Authorization"].value == REDACTED
    assert by_key["Authorization"].redacted is True
    assert by_key["Authorization"].value_size == len(headers[0].value or b"")
    assert by_key["x-api-key"].redacted is True
    assert (by_key["trace-id"].value, by_key["trace-id"].redacted) == ("t-1", False)
    assert by_key["empty"].encoding is ContentEncoding.NULL
    assert by_key["bin"].encoding is ContentEncoding.BINARY
    assert by_key["bin"].value is None


def test_long_keys_and_header_values_are_truncated() -> None:
    policy = SanitizationPolicy(max_key_bytes=4, max_header_value_bytes=3)
    result = sanitize_message(
        _message(key=b"abcdefgh", headers=(RawHeader("h", b"123456"),)), policy
    )
    assert (result.key, result.key_truncated, result.key_size) == ("abcd", True, 8)
    assert (result.headers[0].value, result.headers[0].truncated) == ("123", True)


def test_binary_key_is_not_decoded() -> None:
    result = sanitize_message(_message(key=b"\xff\x00"), SanitizationPolicy())
    assert (result.key, result.key_encoding, result.key_size) == (
        None,
        ContentEncoding.BINARY,
        2,
    )


def test_truncation_respects_utf8_boundaries() -> None:
    assert truncate_utf8("ção", 3) == ("ç", True)  # "ç"=2 bytes, "ã" would split
    assert truncate_utf8("abc", 3) == ("abc", False)


@pytest.mark.parametrize(
    "field", ["max_payload_bytes", "max_key_bytes", "max_header_value_bytes"]
)
def test_policy_limits_must_be_positive(field: str) -> None:
    with pytest.raises(InvalidInputError):
        SanitizationPolicy(**{field: 0})  # type: ignore[arg-type]


def test_raw_message_repr_never_shows_payload() -> None:
    secret = random_credential()
    raw = json.dumps({"password": secret}).encode()
    text = repr(_message(raw))
    assert secret not in text
    assert f"value_size={len(raw)}" in text


def test_payloads_above_the_decode_limit_are_never_decoded() -> None:
    policy = SanitizationPolicy(
        payloads_enabled=True, max_payload_bytes=10, max_decode_bytes=100
    )
    big = json.dumps({"password": random_credential(), "x": "y" * 500}).encode()
    result = sanitize_message(_message(big), policy)
    assert result.payload_treatment is PayloadTreatment.TOO_LARGE
    assert result.payload_preview is None
    assert result.payload_size == len(big)


def test_payload_dropped_by_the_reader_is_reported_as_too_large() -> None:
    message = RawMessage(
        topic="t",
        partition=0,
        offset=1,
        timestamp_ms=1,
        timestamp_type=TimestampType.CREATE_TIME,
        key=None,
        value=b"",
        original_value_size=5_000_000,
    )
    result = sanitize_message(message, ENABLED)
    assert result.payload_treatment is PayloadTreatment.TOO_LARGE
    assert result.payload_size == 5_000_000
    assert message.value_truncated is True


def test_huge_keys_are_decoded_only_up_to_the_limit() -> None:
    key = ("ç" * 10_000).encode()  # 20,000 bytes
    result = sanitize_message(_message(key=key), SanitizationPolicy(max_key_bytes=5))
    assert result.key == "çç"  # a character cut by the limit is dropped
    assert (result.key_truncated, result.key_size) == (True, 20_000)


def test_decode_limit_must_cover_the_preview_limit() -> None:
    with pytest.raises(InvalidInputError, match="max_decode_bytes"):
        SanitizationPolicy(max_payload_bytes=2048, max_decode_bytes=1024)
