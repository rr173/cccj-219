"""Job output: a typed relation package with a collision-free digest.

Output rows stream into a temporary SQLite database so large results never
accumulate in memory.  On success the relation is atomically packaged into a
self-contained ``.bpkg`` file whose digest covers the typed schema and every
typed row with the same domain-separated encoding as read evidence.
"""

from __future__ import annotations

import os
import sqlite3
import struct
from dataclasses import dataclass
from typing import Any, Iterator, Optional

from .encoding import (
    MISSING,
    StreamHasher,
    canonical_json,
    encode_schema,
)
from .snapshot import VALID_TYPES, _uleb128  # type: ignore[attr-defined]

PACKAGE_MAGIC = b"BCACHEPKG1"


class OutputError(ValueError):
    """Raised when a job produces output violating its declared schema."""


class OutputSink:
    """Given to a job; it must declare one schema then write typed rows."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.executescript(
            """
            CREATE TABLE schema_cols (
                ordinal INTEGER PRIMARY KEY,
                col_name TEXT NOT NULL,
                col_type TEXT NOT NULL
            );
            CREATE TABLE rows_out (
                row_idx INTEGER PRIMARY KEY,
                row_id INTEGER,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """
        )
        self._schema: Optional[list[tuple[str, str]]] = None
        self._type_map: dict[str, str] = {}
        self._row_count = 0
        self._hasher: Optional[StreamHasher] = None
        self._closed = False

    def declare(self, schema: list[tuple[str, str]]) -> None:
        if self._schema is not None:
            raise OutputError("output schema declared twice")
        if not schema:
            raise OutputError("output relation needs at least one column")
        seen: set[str] = set()
        for name, col_type in schema:
            if not isinstance(name, str) or not name:
                raise OutputError("invalid output column name")
            if name in seen:
                raise OutputError("duplicate output column %r" % name)
            if col_type not in VALID_TYPES:
                raise OutputError("unsupported output type %r" % col_type)
            seen.add(name)
        self._schema = list(schema)
        self._type_map = dict(schema)
        self._conn.executemany(
            "INSERT INTO schema_cols(ordinal, col_name, col_type) VALUES (?, ?, ?)",
            [(i, n, t) for i, (n, t) in enumerate(schema)],
        )
        self._hasher = StreamHasher(namespace="output-relation-v1")
        self._hasher.update_encoded(encode_schema(schema))
        self._conn.commit()

    def write_row(self, values: dict[str, Any], row_id: Optional[int] = None) -> None:
        if self._closed:
            raise OutputError("sink is closed")
        if self._schema is None:
            raise OutputError("declare() the output schema before writing rows")
        assert self._hasher is not None
        expected = set(self._type_map)
        if set(values) != expected:
            missing = expected - set(values)
            extra = set(values) - expected
            raise OutputError(
                "output row columns mismatch (missing=%s extra=%s)"
                % (sorted(missing), sorted(extra))
            )
        checked: dict[str, Any] = {}
        for name, col_type in self._schema:
            value = values[name]
            if value is MISSING:
                raise OutputError("output cell %r cannot be MISSING" % name)
            if value is None:
                checked[name] = None
                continue
            if col_type == "int64":
                if isinstance(value, bool) or not isinstance(value, int):
                    raise OutputError("output %r must be int64" % name)
                if not (-(2**63) <= value <= 2**63 - 1):
                    raise OutputError("output %r int64 out of range" % name)
            elif col_type == "text":
                if not isinstance(value, str):
                    raise OutputError("output %r must be text" % name)
            else:
                if not isinstance(value, bool):
                    raise OutputError("output %r must be bool" % name)
            checked[name] = value
        self._hasher.update_encoded(canonical_json(_json_safe(checked)))
        self._conn.execute(
            "INSERT INTO rows_out(row_idx, row_id, payload_json) VALUES (?, ?, ?)",
            (self._row_count, row_id, canonical_json(_json_safe(checked)).decode()),
        )
        self._row_count += 1

    def close(self) -> tuple[list[tuple[str, str]], str, int]:
        if self._schema is None:
            raise OutputError("job finished without declaring output schema")
        assert self._hasher is not None
        digest = self._hasher.hexdigest()
        self._conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('output_digest', ?)",
            (digest,),
        )
        self._conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('row_count', ?)",
            (str(self._row_count),),
        )
        self._conn.commit()
        self._conn.close()
        self._closed = True
        return list(self._schema), digest, self._row_count

    @property
    def row_count(self) -> int:
        return self._row_count


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _restore(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _restore(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_restore(v) for v in value]
    return value


@dataclass(frozen=True)
class OutputPackage:
    schema: tuple[tuple[str, str], ...]
    rows: tuple[dict[str, Any], ...]
    digest: str
    row_count: int

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.rows)


def package_from_db(db_path: str, package_path: str) -> OutputPackage:
    """Atomically write a self-contained package from a finished output DB."""
    conn = sqlite3.connect(db_path)
    schema_rows = conn.execute(
        "SELECT col_name, col_type FROM schema_cols ORDER BY ordinal"
    ).fetchall()
    schema = [(r[0], r[1]) for r in schema_rows]
    out_rows = conn.execute(
        "SELECT payload_json FROM rows_out ORDER BY row_idx"
    ).fetchall()
    digest = conn.execute(
        "SELECT value FROM meta WHERE key = 'output_digest'"
    ).fetchone()[0]
    conn.close()

    import json

    rows = tuple(_restore(json.loads(r[0])) for r in out_rows)
    tmp_path = package_path + ".tmp"
    with open(tmp_path, "wb") as fh:
        fh.write(PACKAGE_MAGIC)
        schema_bytes = canonical_json(
            [{"name": n, "type": t} for n, t in schema]
        )
        fh.write(_uleb128(len(schema_bytes)) + schema_bytes)
        fh.write(_uleb128(len(rows)))
        for row in rows:
            payload = canonical_json(_json_safe(row))
            fh.write(_uleb128(len(payload)) + payload)
        fh.write(struct.pack("<32s", bytes.fromhex(digest)))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, package_path)
    return OutputPackage(tuple(schema), rows, digest, len(rows))


def read_package(package_path: str) -> OutputPackage:
    """Read back a package; old-snapshot outputs stay readable on their own."""
    import json

    with open(package_path, "rb") as fh:
        magic = fh.read(len(PACKAGE_MAGIC))
        if magic != PACKAGE_MAGIC:
            raise OutputError("not a bcache output package: %s" % package_path)

        def read_frame() -> bytes:
            n = _read_uleb(fh)
            return fh.read(n)

        schema_payload = json.loads(read_frame().decode("utf-8"))
        schema = tuple((c["name"], c["type"]) for c in schema_payload)
        count = _read_uleb(fh)
        rows = []
        for _ in range(count):
            rows.append(_restore(json.loads(read_frame().decode("utf-8"))))
        digest = struct.unpack("<32s", fh.read(32))[0].hex()

    pkg = OutputPackage(schema, tuple(rows), digest, count)
    if digest != digest_package(pkg):
        raise OutputError("package digest mismatch (corrupted or tampered file)")
    return pkg


def digest_package(pkg: OutputPackage) -> str:
    hasher = StreamHasher(namespace="output-relation-v1")
    hasher.update_encoded(encode_schema(list(pkg.schema)))
    for row in pkg.rows:
        hasher.update_encoded(canonical_json(_json_safe(row)))
    return hasher.hexdigest()


def _read_uleb(fh) -> int:
    result = 0
    shift = 0
    while True:
        byte = fh.read(1)
        if not byte:
            raise OutputError("truncated package")
        b = byte[0]
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result
        shift += 7
