"""Sanitization of messages before they reach MCP clients.

Rules:

- Payloads are omitted unless the operator enabled them; their size is
  always reported.
- In JSON payloads, the value of any field whose name looks sensitive is
  replaced with "[REDACTED]", recursively. The matched paths are reported.
- Headers whose name looks sensitive have their value withheld.
- Keys, header values and payload previews are truncated to byte limits.
- Binary (non-UTF-8) content is never decoded; only its size is reported.

Field-name matching cannot see secrets inside free text, so non-JSON text
payloads are only truncated. Responses say which treatment was applied.
"""

import codecs
import json
import re
from dataclasses import dataclass
from enum import StrEnum

from streamsage.domain.entities.message import RawHeader, RawMessage, TimestampType
from streamsage.domain.exceptions import InvalidInputError

REDACTED = "[REDACTED]"
MAX_REPORTED_REDACTIONS = 20

# Matched as substrings of normalized names (lowercase, alphanumerics only),
# so "user_password", "accessToken" and "X-Api-Key" all match.
DEFAULT_SENSITIVE_TERMS = frozenset(
    {
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "authorization",
        "apikey",
        "accesskey",
        "privatekey",
        "credential",
        "cookie",
        "cpf",
        "cnpj",
        "ssn",
        "email",
        "phone",
        "creditcard",
        "cardnumber",
        "cvv",
        "iban",
    }
)

_NON_ALNUM = re.compile(r"[^a-z0-9]")


def normalize_name(name: str) -> str:
    return _NON_ALNUM.sub("", name.lower())


class PayloadTreatment(StrEnum):
    OMITTED = "omitted"  # payloads disabled by configuration
    NULL = "null"  # no payload (for example a tombstone)
    JSON_REDACTED = "json_redacted"  # sensitive fields redacted, then truncated
    TEXT_TRUNCATED = "text_truncated"  # not JSON: truncated only
    BINARY = "binary"  # not UTF-8: not decoded
    TOO_LARGE = "too_large"  # above max_decode_bytes: never decoded or parsed


class ContentEncoding(StrEnum):
    TEXT = "text"
    BINARY = "binary"
    NULL = "null"


@dataclass(frozen=True, slots=True)
class SanitizationPolicy:
    payloads_enabled: bool = False
    # Keys often carry identifiers (customer ids, emails). When disabled,
    # responses report only the key's size and encoding.
    keys_enabled: bool = True
    max_payload_bytes: int = 1024
    max_key_bytes: int = 256
    max_header_value_bytes: int = 256
    # Payloads larger than this are not decoded or parsed at all, which bounds
    # the CPU and memory spent on a single message.
    max_decode_bytes: int = 256 * 1024
    sensitive_terms: frozenset[str] = DEFAULT_SENSITIVE_TERMS

    def __post_init__(self) -> None:
        for name in (
            "max_payload_bytes",
            "max_key_bytes",
            "max_header_value_bytes",
            "max_decode_bytes",
        ):
            if getattr(self, name) < 1:
                raise InvalidInputError(f"{name} must be at least 1.")
        if self.max_decode_bytes < self.max_payload_bytes:
            raise InvalidInputError(
                "max_decode_bytes must be at least max_payload_bytes."
            )
        normalized = frozenset(
            term for term in (normalize_name(t) for t in self.sensitive_terms) if term
        )
        object.__setattr__(self, "sensitive_terms", normalized)

    def is_sensitive(self, name: str) -> bool:
        normalized = normalize_name(name)
        return any(term in normalized for term in self.sensitive_terms)


@dataclass(frozen=True, slots=True)
class InspectedHeader:
    key: str
    value: str | None
    value_size: int | None
    encoding: ContentEncoding
    redacted: bool
    truncated: bool


@dataclass(frozen=True, slots=True)
class InspectedMessage:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    timestamp_type: TimestampType
    key: str | None
    key_size: int | None
    key_encoding: ContentEncoding
    key_truncated: bool
    headers: tuple[InspectedHeader, ...]
    payload_size: int | None
    payload_treatment: PayloadTreatment
    payload_preview: str | None
    payload_truncated: bool
    redacted_fields: tuple[str, ...]
    key_withheld: bool = False


def sanitize_message(
    message: RawMessage, policy: SanitizationPolicy
) -> InspectedMessage:
    key, key_encoding, key_truncated = _decode_limited(
        message.key, policy.max_key_bytes
    )
    key_withheld = message.key is not None and not policy.keys_enabled
    if key_withheld:
        key, key_truncated = None, False
    payload = _sanitize_payload(message, policy)
    return InspectedMessage(
        topic=message.topic,
        partition=message.partition,
        offset=message.offset,
        timestamp_ms=message.timestamp_ms,
        timestamp_type=message.timestamp_type,
        key=key,
        key_size=None if message.key is None else len(message.key),
        key_encoding=key_encoding,
        key_truncated=key_truncated,
        headers=tuple(_sanitize_header(h, policy) for h in message.headers),
        payload_size=message.value_size,
        payload_treatment=payload.treatment,
        payload_preview=payload.preview,
        payload_truncated=payload.truncated,
        redacted_fields=payload.redacted_fields,
        key_withheld=key_withheld,
    )


@dataclass(frozen=True, slots=True)
class _Payload:
    treatment: PayloadTreatment
    preview: str | None = None
    truncated: bool = False
    redacted_fields: tuple[str, ...] = ()


def _sanitize_payload(message: RawMessage, policy: SanitizationPolicy) -> _Payload:
    value = message.value
    if value is None:
        return _Payload(PayloadTreatment.NULL)
    if not policy.payloads_enabled:
        return _Payload(PayloadTreatment.OMITTED)
    size = message.value_size or 0
    if message.value_truncated or size > policy.max_decode_bytes:
        return _Payload(PayloadTreatment.TOO_LARGE)
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError:
        return _Payload(PayloadTreatment.BINARY)
    try:
        document = json.loads(text)
    except ValueError:
        preview, truncated = truncate_utf8(text, policy.max_payload_bytes)
        return _Payload(PayloadTreatment.TEXT_TRUNCATED, preview, truncated)
    redacted_paths: list[str] = []
    cleaned = _redact(document, "", policy, redacted_paths)
    serialized = json.dumps(cleaned, ensure_ascii=False, separators=(",", ":"))
    preview, truncated = truncate_utf8(serialized, policy.max_payload_bytes)
    return _Payload(
        PayloadTreatment.JSON_REDACTED,
        preview,
        truncated,
        tuple(redacted_paths[:MAX_REPORTED_REDACTIONS]),
    )


JsonValue = object


def _redact(
    node: JsonValue, path: str, policy: SanitizationPolicy, redacted: list[str]
) -> JsonValue:
    if isinstance(node, dict):
        result: dict[str, JsonValue] = {}
        for key, value in node.items():
            child = f"{path}.{key}" if path else str(key)
            if policy.is_sensitive(str(key)):
                result[key] = REDACTED
                redacted.append(child)
            else:
                result[key] = _redact(value, child, policy, redacted)
        return result
    if isinstance(node, list):
        return [
            _redact(item, f"{path}[{index}]", policy, redacted)
            for index, item in enumerate(node)
        ]
    return node


def _sanitize_header(header: RawHeader, policy: SanitizationPolicy) -> InspectedHeader:
    size = None if header.value is None else len(header.value)
    if header.value is not None and policy.is_sensitive(header.key):
        return InspectedHeader(
            key=header.key,
            value=REDACTED,
            value_size=size,
            encoding=ContentEncoding.TEXT,
            redacted=True,
            truncated=False,
        )
    value, encoding, truncated = _decode_limited(
        header.value, policy.max_header_value_bytes
    )
    return InspectedHeader(
        key=header.key,
        value=value,
        value_size=size,
        encoding=encoding,
        redacted=False,
        truncated=truncated,
    )


def _decode_limited(
    raw: bytes | None, max_bytes: int
) -> tuple[str | None, ContentEncoding, bool]:
    """Decode at most ``max_bytes`` of ``raw`` as UTF-8.

    Only the needed prefix is decoded, so a huge key or header costs no more
    than a small one. A multi-byte character cut by the limit is dropped.
    """
    if raw is None:
        return None, ContentEncoding.NULL, False
    truncated = len(raw) > max_bytes
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        text = decoder.decode(raw[:max_bytes], final=not truncated)
    except UnicodeDecodeError:
        return None, ContentEncoding.BINARY, False
    return text, ContentEncoding.TEXT, truncated


def truncate_utf8(text: str, max_bytes: int) -> tuple[str, bool]:
    """Cut ``text`` to at most ``max_bytes`` UTF-8 bytes, on a character boundary."""
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text, False
    return encoded[:max_bytes].decode("utf-8", errors="ignore"), True
