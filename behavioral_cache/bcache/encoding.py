"""Explicit, collision-free typed encoding and hashing primitives.

The project must never confuse ``true`` with ``1``, ``null`` with a missing
field, or two concatenated strings with one another.  Every value is encoded
with a one-byte *domain tag* plus a length-prefixed payload, so encodings of
different shapes or types can never collide.

All hashes are BLAKE2b (32-byte digest).  Hashing is exposed both as a
one-shot helper and as a streaming :class:`StreamHasher` so large read results
never have to be collected in memory.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, BinaryIO, Iterable, Optional

DIGEST_BYTES = 32

# Domain-separation tags.  Tags are single bytes and must stay unique.
TAG_INT = b"i"
TAG_TEXT = b"s"
TAG_BOOL = b"b"
TAG_NULL = b"n"
TAG_MISSING = b"m"  # distinct from NULL: the column does not exist in the row
TAG_LIST = b"L"
TAG_RECORD = b"R"
TAG_SCHEMA = b"C"  # a typed column: (name, type)
TAG_NAMESPACE = b"#"  # domain prefix when hashing heterogeneous streams

MISSING = object()
"""Sentinel for a field that is absent (as opposed to a SQL/payload NULL)."""


class EncodingError(TypeError):
    """Raised when a Python value cannot be encoded under the value model."""


def _uleb128(n: int) -> bytes:
    if n < 0:
        raise EncodingError("negative length")
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def encode_value(value: Any) -> bytes:
    """Encode one of int64 / str / bool / None / MISSING into tagged bytes.

    ``bool`` is checked before ``int`` deliberately: Python's ``True`` is an
    ``int`` but the data model treats them as different domains.
    """
    if value is None:
        return TAG_NULL
    if value is MISSING:
        return TAG_MISSING
    if isinstance(value, bool):
        return TAG_BOOL + (b"\x01" if value else b"\x00")
    if isinstance(value, int):
        if not (-(2**63) <= value <= 2**63 - 1):
            raise EncodingError("integer outside int64 range: %r" % (value,))
        return TAG_INT + str(value).encode("utf-8") + b"\n"
    if isinstance(value, str):
        payload = value.encode("utf-8")
        return TAG_TEXT + _uleb128(len(payload)) + payload
    raise EncodingError("unsupported value type: %r" % (type(value),))


def encode_tuple(values: Iterable[Any]) -> bytes:
    """Encode an ordered sequence of values with length framing."""
    parts = [TAG_LIST]
    n = 0
    for v in values:
        parts.append(encode_value(v))
        n += 1
    return b"".join([TAG_LIST, _uleb128(n)] + parts)


def encode_record(record: dict) -> bytes:
    """Encode a string->value mapping with sorted keys and field framing."""
    parts = [TAG_RECORD, _uleb128(len(record))]
    for key in sorted(record):
        if not isinstance(key, str):
            raise EncodingError("record keys must be strings")
        k = key.encode("utf-8")
        parts.append(_uleb128(len(k)) + k)
        parts.append(encode_value(record[key]))
    return b"".join(parts)


def encode_typed_column(name: str, col_type: str) -> bytes:
    n = name.encode("utf-8")
    t = col_type.encode("utf-8")
    return TAG_SCHEMA + _uleb128(len(n)) + n + _uleb128(len(t)) + t


def encode_schema(schema: list[tuple[str, str]]) -> bytes:
    parts = [TAG_LIST, _uleb128(len(schema))]
    for name, col_type in schema:
        parts.append(encode_typed_column(name, col_type))
    return b"".join(parts)


def digest_bytes(parts: Iterable[bytes]) -> str:
    """One-shot hash over already-encoded chunks."""
    h = hashlib.blake2b(digest_size=DIGEST_BYTES)
    for part in parts:
        h.update(part)
    return h.hexdigest()


def digest_values(values: Iterable[Any]) -> str:
    h = hashlib.blake2b(digest_size=DIGEST_BYTES)
    for v in values:
        h.update(encode_value(v))
    return h.hexdigest()


def digest_schema(schema: list[tuple[str, str]]) -> str:
    return digest_bytes([encode_schema(schema)])


class StreamHasher:
    """Incremental hasher for large, never-materialised result streams."""

    def __init__(self, namespace: Optional[str] = None) -> None:
        self._h = hashlib.blake2b(digest_size=DIGEST_BYTES)
        self.count = 0
        if namespace is not None:
            payload = namespace.encode("utf-8")
            self._h.update(TAG_NAMESPACE + _uleb128(len(payload)) + payload)

    def update_bytes(self, data: bytes) -> None:
        self._h.update(data)

    def update_value(self, value: Any) -> None:
        self._h.update(encode_value(value))
        self.count += 1

    def update_encoded(self, chunk: bytes) -> None:
        self._h.update(chunk)
        self.count += 1

    def hexdigest(self) -> str:
        return self._h.hexdigest()


def canonical_json(obj: Any) -> bytes:
    """Stable JSON encoding for params / dependencies / call arguments.

    Tuples become JSON arrays, dict keys are sorted, and no whitespace is
    emitted, so equal Python structures always produce identical bytes.
    """

    def default(o: Any) -> Any:
        if o is MISSING:
            return {"__tag__": "missing"}
        if isinstance(o, (bytes, bytearray)):
            return {"__tag__": "bytes", "hex": bytes(o).hex()}
        raise EncodingError("cannot canonicalize %r" % (type(o),))

    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=default,
    ).encode("utf-8")


def write_stream(fh: BinaryIO, chunks: Iterable[bytes]) -> None:
    for chunk in chunks:
        fh.write(chunk)
