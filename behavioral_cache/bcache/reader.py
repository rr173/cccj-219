"""Reader interface and the recording wrapper used by real job executions.

The reader is the *only* way a job can observe data.  Five kinds of
observations are kept distinct:

* table does not exist (``exists`` -> False; ``read``/``lookup`` error)
* table exists but is empty
* table exists, key column known, but the key is absent
* requested column is not in the schema
* a cell value is NULL (typed ``None``), distinct from a missing cell
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

from . import predicate as pred
from .encoding import (
    MISSING,
    StreamHasher,
    canonical_json,
    digest_schema,
    encode_value,
)
from .errors import TableNotFound, UnknownColumn
from .evidence import EvidenceWriter, project_row
from .snapshot import Snapshot, _decode_cell  # type: ignore[attr-defined]


@dataclass(frozen=True)
class Row:
    """One delivered row. Typed values; a missing cell is ``MISSING``."""

    row_id: int
    values: dict[str, Any]
    schema: tuple[tuple[str, str], ...]

    def __getitem__(self, column: str) -> Any:
        return self.values[column]

    def get(self, column: str, default: Any = None) -> Any:
        return self.values.get(column, default)

    def as_dict(self) -> dict[str, Any]:
        return dict(self.values)


@dataclass(frozen=True)
class LookupResult:
    table: str
    key_col: Optional[str]
    key_value: Any
    status: str  # found | key_not_found | table_not_found | empty_table | schema_error
    value: Optional[dict[str, Any]] = None
    schema: tuple[tuple[str, str], ...] = ()
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReadOutcome:
    """Everything observable about one completed read, in delivery order."""

    table: str
    schema: tuple[tuple[str, str], ...]
    rows: tuple[Row, ...]
    result_digest: str


def _framed(hasher: StreamHasher, label: str, payload: bytes) -> None:
    tag = label.encode("utf-8")
    from .encoding import _uleb128  # type: ignore[attr-defined]

    hasher.update_encoded(_uleb128(len(tag)) + tag)
    hasher.update_encoded(payload)


def hash_read_stream(
    schema: list[tuple[str, str]],
) -> StreamHasher:
    """Hasher framing schema then each projected row (caller adds rows)."""
    hasher = StreamHasher(namespace="read-observation-v1")
    from .encoding import _uleb128  # type: ignore[attr-defined]

    _framed(hasher, "schema", digest_schema(schema).encode("ascii"))
    return hasher


def hash_read_row(hasher: StreamHasher, row_id: int, projection: dict) -> None:
    from .encoding import _uleb128  # type: ignore[attr-defined]

    _framed(hasher, "row_id", encode_value(row_id))
    _framed(hasher, "projection", canonical_json(_json_safe(projection)))


def _json_safe(value: Any) -> Any:
    if value is MISSING:
        return {"__tag__": "missing"}
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


class Reader:
    """Direct reader over a pinned immutable snapshot."""

    def __init__(self, snapshot: Snapshot, conn: sqlite3.Connection):
        self.snapshot = snapshot
        self.conn = conn

    # -- exists -------------------------------------------------------------

    def exists(self, table: str) -> bool:
        return self.snapshot.has_table(self.conn, table)

    # -- list_partitions ----------------------------------------------------

    def list_partitions(self, prefix: str) -> list[str]:
        if not isinstance(prefix, str):
            raise TypeError("prefix must be a string")
        return self.snapshot.list_partitions(self.conn, prefix)

    # -- read ---------------------------------------------------------------

    def _prepare_read(
        self, table: str, predicate_obj: Any, columns: list[str]
    ) -> tuple[list[tuple[str, str]], list[str]]:
        schema = self.snapshot.schema(self.conn, table)  # may raise TableNotFound
        col_names = [c for c, _ in schema]
        if not columns:
            raise ValueError("columns must be a non-empty list")
        for col in columns:
            if col not in col_names:
                raise UnknownColumn(table, col)
        if predicate_obj is not None:
            pred.validate(predicate_obj, col_names)
        return schema, col_names

    def read(
        self,
        table: str,
        predicate_obj: Any = None,
        columns: Optional[list[str]] = None,
    ) -> Iterator[Row]:
        if columns is None:
            raise ValueError("columns is required")
        schema, _ = self._prepare_read(table, predicate_obj, columns)
        schema_t = tuple(schema)
        for row_id, full_row in self.snapshot.scan(self.conn, table):
            if predicate_obj is not None:
                match = pred.evaluate(predicate_obj, full_row)
                if match is not True:
                    continue
            yield Row(row_id, project_row(full_row, columns), schema_t)

    def read_all(
        self,
        table: str,
        predicate_obj: Any = None,
        columns: Optional[list[str]] = None,
    ) -> ReadOutcome:
        if columns is None:
            raise ValueError("columns is required")
        schema, _ = self._prepare_read(table, predicate_obj, columns)
        hasher = hash_read_stream(schema)
        delivered: list[Row] = []
        schema_t = tuple(schema)
        for row_id, full_row in self.snapshot.scan(self.conn, table):
            if predicate_obj is not None:
                match = pred.evaluate(predicate_obj, full_row)
                if match is not True:
                    continue
            projection = project_row(full_row, columns)
            hash_read_row(hasher, row_id, projection)
            delivered.append(Row(row_id, projection, schema_t))
        return ReadOutcome(table, schema_t, tuple(delivered), hasher.hexdigest())

    # -- lookup -------------------------------------------------------------

    def lookup(
        self,
        table: str,
        key_value: Any,
        key_col: Optional[str] = None,
    ) -> LookupResult:
        if not self.snapshot.has_table(self.conn, table):
            return LookupResult(
                table, key_col, key_value, "table_not_found",
                detail={"error": "TableNotFound"},
            )
        schema = self.snapshot.schema(self.conn, table)
        declared_key = self.snapshot.key_col(self.conn, table)
        if key_col is None:
            key_col = declared_key
        if key_col is None:
            return LookupResult(
                table, None, key_value, "schema_error",
                schema=tuple(schema),
                detail={"error": "no key column declared for table %r" % table},
            )
        col_names = [c for c, _ in schema]
        if key_col not in col_names:
            return LookupResult(
                table, key_col, key_value, "schema_error",
                schema=tuple(schema),
                detail={"error": "UnknownColumn", "column": key_col},
            )
        if self.snapshot.count_rows(self.conn, table) == 0:
            return LookupResult(
                table, key_col, key_value, "empty_table",
                schema=tuple(schema),
            )
        row = self._fetch_row(table, key_col, key_value, col_names)
        if row is None:
            return LookupResult(
                table, key_col, key_value, "key_not_found",
                schema=tuple(schema),
            )
        return LookupResult(
            table, key_col, key_value, "found",
            value=row, schema=tuple(schema),
        )

    def _fetch_row(
        self,
        table: str,
        key_col: str,
        key_value: Any,
        col_names: list[str],
    ) -> Optional[dict[str, Any]]:
        placeholders = ",".join("?" for _ in col_names)
        rows = self.conn.execute(
            "SELECT row_id, col_name, value_type, int_value, text_value, "
            "bool_value FROM cells WHERE table_name = ? AND col_name IN (%s) "
            "ORDER BY row_id, col_name" % placeholders,
            (table, *col_names),
        ).fetchall()
        candidate: dict[str, Any] = {c: MISSING for c in col_names}
        current_id: Any = None
        current: dict[str, Any] = {}
        target_id: Any = None
        for raw in rows:
            rid = raw[0]
            if rid != current_id:
                if current_id is not None and target_id == current_id:
                    return current
                current_id = rid
                current = {c: MISSING for c in col_names}
            current[raw[1]] = _decode_cell(raw[2], raw[3], raw[4], raw[5])
            if raw[1] == key_col and current.get(key_col) == key_value:
                target_id = rid
        if target_id is not None and target_id == current_id:
            return current
        return None


# ---------------------------------------------------------------------------
# Recording reader
# ---------------------------------------------------------------------------

def json_loads(data: bytes) -> Any:
    import json

    return json.loads(data.decode("utf-8"))


class RecordingReader:
    """Wraps a :class:`Reader` and records every observable call in order."""

    def __init__(self, reader: Reader, evidence: EvidenceWriter):
        self._reader = reader
        self._evidence = evidence
        self.call_count = 0

    @property
    def reader(self) -> Reader:
        return self._reader

    # -- exists -------------------------------------------------------------

    def exists(self, table: str) -> bool:
        seq = self._evidence.begin_call()
        self.call_count += 1
        found = self._reader.exists(table)
        hasher = StreamHasher(namespace="exists-observation-v1")
        _framed(hasher, "table", encode_value(table))
        _framed(hasher, "exists", encode_value(found))
        self._evidence.write_simple_call(
            seq,
            "exists",
            {"table": table},
            "ok",
            hasher.hexdigest(),
            {"exists": found},
        )
        return found

    # -- list_partitions ----------------------------------------------------

    def list_partitions(self, prefix: str) -> list[str]:
        seq = self._evidence.begin_call()
        self.call_count += 1
        names = self._reader.list_partitions(prefix)
        hasher = StreamHasher(namespace="partitions-observation-v1")
        _framed(hasher, "prefix", encode_value(prefix))
        _framed(hasher, "count", encode_value(len(names)))
        for name in names:
            _framed(hasher, "name", encode_value(name))
        self._evidence.write_simple_call(
            seq,
            "list_partitions",
            {"prefix": prefix},
            "ok",
            hasher.hexdigest(),
            {"partitions": names, "count": len(names)},
        )
        return names

    # -- lookup -------------------------------------------------------------

    def lookup(self, table: str, key_value: Any, key_col: Optional[str] = None):
        seq = self._evidence.begin_call()
        self.call_count += 1
        args = {"table": table, "key_value": _json_safe(key_value)}
        if key_col is not None:
            args["key_col"] = key_col
        result = self._reader.lookup(table, key_value, key_col)
        args["key_col"] = result.key_col
        hasher = StreamHasher(namespace="lookup-observation-v1")
        _framed(hasher, "table", encode_value(table))
        _framed(hasher, "key_col", encode_value(
            result.key_col if result.key_col is not None else MISSING))
        _framed(hasher, "key_value", encode_value(key_value))
        _framed(hasher, "status", encode_value(result.status))
        if result.status == "found":
            _framed(hasher, "schema", digest_schema(list(result.schema)).encode("ascii"))
            for col, _ in result.schema:
                _framed(hasher, "cell:" + col, encode_value(result.value[col]))
        detail = {
            "status": result.status,
            "schema": [{"name": n, "type": t} for n, t in result.schema],
        }
        if result.status == "found":
            detail["value"] = _json_safe(result.value)
        if result.detail:
            detail["error_detail"] = result.detail
        outcome = "error" if result.status in ("table_not_found", "schema_error") else "ok"
        error_type = result.detail.get("error") if outcome == "error" else None
        self._evidence.write_simple_call(
            seq, "lookup", args, outcome, hasher.hexdigest(), detail, error_type
        )
        return result

    # -- read ---------------------------------------------------------------

    def read(self, table: str, predicate_obj: Any = None, columns: Optional[list[str]] = None):
        if columns is None:
            raise ValueError("columns is required")
        seq = self._evidence.begin_call()
        self.call_count += 1
        args = {
            "table": table,
            "predicate": None
            if predicate_obj is None
            else json_loads(pred.canonical(predicate_obj)),
            "columns": list(columns),
        }
        return self._stream_read(seq, table, predicate_obj, columns, args)

    def _stream_read(self, seq, table, predicate_obj, columns, args):
        hasher: Optional[StreamHasher] = None
        row_idx = 0
        schema: list[tuple[str, str]] = []
        try:
            schema, _ = self._reader._prepare_read(table, predicate_obj, columns)
        except (TableNotFound, UnknownColumn) as exc:
            error_type = type(exc).__name__
            self._evidence.write_simple_call(
                seq,
                "read",
                args,
                "error",
                StreamHasher(namespace="read-error-v1").hexdigest(),
                {"error": error_type, "table": table,
                 "column": getattr(exc, "column", None)},
                error_type,
            )
            raise
        self._evidence.begin_read_call(seq, args, schema)
        hasher = hash_read_stream(schema)
        try:
            for row in self._reader.read(table, predicate_obj, columns):
                hash_read_row(hasher, row.row_id, row.values)
                self._evidence.write_read_row(
                    seq, row_idx, row.row_id, _json_safe(row.values)
                )
                row_idx += 1
                yield row
        except GeneratorExit:
            # Job abandoned the iterator: record exactly how far it observed.
            self._evidence.finish_read_call(seq, hasher.hexdigest(), row_idx, True)
            raise
        else:
            self._evidence.finish_read_call(seq, hasher.hexdigest(), row_idx, False)
