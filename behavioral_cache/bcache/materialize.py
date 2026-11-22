"""Materialise an "as of" snapshot from a base snapshot plus a corrections log.

The operator names one base snapshot, one unordered JSONL corrections log, a
business instant ``T`` and a registration watermark ``S``.  The result is a
normal immutable snapshot that existing jobs can use directly by its id.

Model
-----
Every row of the base schema keeps its identity ``(table, row_id)``.  A
correction is a half-open business-time interval ``[from, to)`` with a
registration number ``seq``.  For one business instant ``T`` we only consider
corrections with ``seq <= S`` whose interval contains ``T``; among those the
highest ``seq`` wins.  A winning ``delete`` removes the row, a winning ``put``
replaces *all* cells of the row.  Rows with no candidate keep their base state.
Because candidates are selected per instant, an expired late correction cannot
shadow an earlier correction (or the base row) that is still applicable.

Resource bounds
---------------
* The log is parsed one line at a time and spilled into a temporary SQLite
  staging database; at most one line's parsed object is resident.
* Only the *winners* for ``T`` are retained (in SQLite), never all log rows.
* The base snapshot is streamed table by table, row by row; a row's cells are
  the only per-row buffer.
* Final-view key uniqueness and the final digest are checked with streaming
  SQLite cursors, never by rescanning the log and never by collecting rows.

Publication is all-or-nothing: the snapshot is fully built and validated in a
scratch directory, then moved into the registry.  ``current`` is never touched.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from typing import Any, Iterator, Optional

from .errors import SnapshotConflict
from .snapshot import Snapshot, SnapshotBuilder, SnapshotRegistry

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


class MaterializeError(ValueError):
    """A corrections request that cannot be satisfied as stated."""


class CorrectionLogError(MaterializeError):
    """Malformed, conflicting or type-invalid corrections log."""


class KeyConflictError(MaterializeError):
    """The delivered view would contain a non-NULL key more than once."""


@dataclass(frozen=True)
class MaterializeResult:
    snapshot: Snapshot
    outcome: str  # "published" | "unchanged"
    effective_at: int
    known_through: int
    base_snapshot_id: str
    corrections_seen: int
    corrections_eligible: int
    winner_rows: int


# ---------------------------------------------------------------------------
# Parsing and validation
# ---------------------------------------------------------------------------

def _is_int(value: Any) -> bool:
    # JSON true/false parse to bool, which Python treats as int: reject it.
    return isinstance(value, int) and not isinstance(value, bool)


def _require_int64(value: Any, what: str, *, non_negative: bool = False) -> int:
    if not _is_int(value):
        raise CorrectionLogError(
            "%s must be an int64 integer, got %r" % (what, type(value).__name__)
        )
    if not (INT64_MIN <= value <= INT64_MAX):
        raise CorrectionLogError("%s is outside the signed 64-bit range" % what)
    if non_negative and value < 0:
        raise CorrectionLogError("%s must be non-negative" % what)
    return value


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise CorrectionLogError("duplicate JSON member %r" % key)
        obj[key] = value
    return obj


def _reject_constant(name: str) -> Any:
    raise CorrectionLogError("non-JSON constant %s is not allowed" % name)


def _parse_json_object(line: bytes, lineno: int) -> dict[str, Any]:
    try:
        text = line.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CorrectionLogError("line %d is not valid UTF-8" % lineno) from exc
    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_constant,
        )
    except json.JSONDecodeError as exc:
        raise CorrectionLogError(
            "line %d is not valid JSON: %s" % (lineno, exc)
        ) from exc
    if not isinstance(parsed, dict):
        raise CorrectionLogError("line %d must be a JSON object" % lineno)
    return parsed


def _check_event(event: dict[str, Any], lineno: int) -> None:
    """Structural/type checks applied to *every* line, visible at T or not."""
    where = "line %d" % lineno
    required = {"seq", "table", "row_id", "from", "to", "op"}
    absent = required - set(event)
    if absent:
        raise CorrectionLogError(
            "%s: missing required fields %s" % (where, sorted(absent))
        )
    table = event["table"]
    if not isinstance(table, str) or not table:
        raise CorrectionLogError("%s: table must be a non-empty string" % where)
    _require_int64(event.get("row_id"), where + ": row_id")
    _require_int64(event.get("seq"), where + ": seq")
    if event["seq"] <= 0:
        raise CorrectionLogError("%s: seq must be a positive integer" % where)
    start = _require_int64(event.get("from"), where + ": from")
    end = event.get("to")
    if end is not None:
        _require_int64(end, where + ": to")
        if not start < end:
            raise CorrectionLogError(
                "%s: finite interval must satisfy from < to" % where
            )
    op = event.get("op")
    if op not in ("put", "delete"):
        raise CorrectionLogError("%s: op must be 'put' or 'delete'" % where)
    allowed = {"seq", "table", "row_id", "from", "to", "op"}
    if op == "put":
        allowed.add("values")
        if "values" not in event:
            raise CorrectionLogError("%s: put requires values" % where)
        if not isinstance(event["values"], dict):
            raise CorrectionLogError("%s: values must be an object" % where)
    elif "values" in event:
        raise CorrectionLogError("%s: delete must not carry values" % where)
    unknown = set(event) - allowed
    if unknown:
        raise CorrectionLogError(
            "%s: unknown fields %s" % (where, sorted(unknown))
        )


def _validate_values(
    table: str,
    row_id: int,
    values: dict[str, Any],
    schema: dict[str, str],
    where: str,
) -> None:
    """Every put carries a complete, correctly typed set of all columns."""
    missing = [c for c in schema if c not in values]
    if missing:
        raise CorrectionLogError(
            "%s: put for %s row %r is missing columns %s"
            % (where, table, row_id, sorted(missing))
        )
    extra = set(values) - set(schema)
    if extra:
        raise CorrectionLogError(
            "%s: put for %s row %r has unknown columns %s"
            % (where, table, row_id, sorted(extra))
        )
    for col, col_type in schema.items():
        value = values[col]
        if value is None:
            continue  # explicit NULL is allowed for every declared type
        if col_type == "bool":
            if not isinstance(value, bool):
                raise CorrectionLogError(
                    "%s: column %s.%s expects bool, got %s"
                    % (where, table, col, type(value).__name__)
                )
        elif col_type == "int64":
            if not _is_int(value):
                raise CorrectionLogError(
                    "%s: column %s.%s expects int64, got %s"
                    % (where, table, col, type(value).__name__)
                )
            if not (INT64_MIN <= value <= INT64_MAX):
                raise CorrectionLogError(
                    "%s: column %s.%s is outside int64" % (where, table, col)
                )
        else:  # text
            if not isinstance(value, str):
                raise CorrectionLogError(
                    "%s: column %s.%s expects text, got %s"
                    % (where, table, col, type(value).__name__)
                )


def _typed_value(value: Any) -> Any:
    """Type-tagged value so ``true`` and ``1`` never compare equal on resend."""
    if isinstance(value, bool):
        return ["bool", value]
    if value is None:
        return ["null"]
    if isinstance(value, int):
        return ["int64", value]
    return ["text", value]


def _canonical_event(event: dict[str, Any]) -> bytes:
    payload = {
        "seq": event["seq"],
        "table": event["table"],
        "row_id": event["row_id"],
        "from": event["from"],
        "to": event["to"],
        "op": event["op"],
        "values": None
        if event["op"] == "delete"
        else {k: _typed_value(v) for k, v in event["values"].items()},
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _log_lines(path: str) -> Iterator[tuple[int, bytes]]:
    with open(path, "rb") as fh:
        for lineno, line in enumerate(fh, start=1):
            if line.strip():
                yield lineno, line


# ---------------------------------------------------------------------------
# Staging database
# ---------------------------------------------------------------------------

_STAGING_SQL = """
CREATE TABLE event (
    seq INTEGER PRIMARY KEY,
    canonical BLOB NOT NULL
);
CREATE TABLE winner (
    table_name TEXT NOT NULL,
    row_id INTEGER NOT NULL,
    op TEXT NOT NULL,
    seq INTEGER NOT NULL,
    values_json TEXT,
    PRIMARY KEY (table_name, row_id)
);
"""


class _Staging:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.executescript(_STAGING_SQL)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def _interval_contains(event: dict[str, Any], instant: int) -> bool:
    if event["from"] > instant:
        return False
    end = event["to"]
    return end is None or instant < end


def _load_log(
    staging: _Staging,
    path: str,
    base_schemas: dict[str, dict[str, str]],
    effective_at: int,
    known_through: int,
) -> tuple[int, int]:
    """Parse/validate every line; spill dedup state and winners to SQLite."""
    conn = staging.conn
    count = 0
    eligible = 0
    for lineno, raw in _log_lines(path):
        count += 1
        event = _parse_json_object(raw, lineno)
        _check_event(event, lineno)
        table = event["table"]
        if table not in base_schemas:
            raise CorrectionLogError(
                "line %d: unknown table %r" % (lineno, table)
            )
        row_id = event["row_id"]
        seq = event["seq"]
        if event["op"] == "put":
            _validate_values(
                table, row_id, event["values"], base_schemas[table],
                "line %d" % lineno,
            )
        canonical = _canonical_event(event)
        existing = conn.execute(
            "SELECT canonical FROM event WHERE seq = ?", (seq,)
        ).fetchone()
        if existing is not None:
            if existing[0] != canonical:
                raise CorrectionLogError(
                    "line %d: seq %d re-sent with different content"
                    % (lineno, seq)
                )
            continue  # identical resend of a registered event
        conn.execute(
            "INSERT INTO event(seq, canonical) VALUES (?, ?)", (seq, canonical)
        )
        if seq <= known_through and _interval_contains(event, effective_at):
            eligible += 1
            _maybe_winner(conn, event)
    conn.commit()
    return count, eligible


def _maybe_winner(conn: sqlite3.Connection, event: dict[str, Any]) -> None:
    table = event["table"]
    row_id = event["row_id"]
    row = conn.execute(
        "SELECT seq FROM winner WHERE table_name = ? AND row_id = ?",
        (table, row_id),
    ).fetchone()
    if row is not None and row[0] >= event["seq"]:
        return
    values_json = (
        None
        if event["op"] == "delete"
        else json.dumps(event["values"], sort_keys=True, ensure_ascii=False)
    )
    conn.execute(
        "INSERT OR REPLACE INTO winner(table_name, row_id, op, seq, values_json) "
        "VALUES (?, ?, ?, ?, ?)",
        (table, row_id, event["op"], event["seq"], values_json),
    )


# ---------------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------------

def _copy_schema(
    base: Snapshot, builder: SnapshotBuilder
) -> dict[str, list[tuple[str, str]]]:
    base_conn = base.connect()
    try:
        schema: dict[str, list[tuple[str, str]]] = {}
        for name in base.table_names(base_conn):
            cols = base.schema(base_conn, name)
            schema[name] = cols
            builder.add_table(name, cols, key_col=base.key_col(base_conn, name))
        return schema
    finally:
        base_conn.close()


def _store_cell_payload(value: Any) -> tuple[str, Any, Any, Any]:
    """Map a JSON value (already type-checked) to typed cell columns."""
    if value is None:
        return ("null", None, None, None)
    if isinstance(value, bool):
        return ("bool", None, None, 1 if value else 0)
    if isinstance(value, int):
        return ("int64", value, None, None)
    return ("text", None, value, None)


def _copy_base_rows(
    base: Snapshot,
    conn: sqlite3.Connection,
    schema: dict[str, list[tuple[str, str]]],
) -> None:
    base_conn = base.connect()
    try:
        for table, cols in schema.items():
            col_names = [c for c, _ in cols]
            placeholders = ",".join("?" for _ in col_names)
            cursor = base_conn.execute(
                "SELECT row_id, col_name, value_type, int_value, text_value, "
                "bool_value FROM cells WHERE table_name = ? AND col_name IN (%s) "
                "ORDER BY row_id, col_name" % placeholders,
                (table, *col_names),
            )
            batch: list[tuple[Any, ...]] = []
            for raw in cursor:
                batch.append((table, *raw))
                if len(batch) >= 1000:
                    _insert_cells(conn, batch)
                    batch.clear()
            if batch:
                _insert_cells(conn, batch)
    finally:
        base_conn.close()
    conn.commit()


def _insert_cells(conn: sqlite3.Connection, batch: list[tuple[Any, ...]]) -> None:
    conn.executemany(
        "INSERT INTO cells(table_name, row_id, col_name, value_type, "
        "int_value, text_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?)",
        batch,
    )


def _apply_winners(
    staging: _Staging,
    conn: sqlite3.Connection,
    schema: dict[str, list[tuple[str, str]]],
) -> None:
    winner_cursor = staging.conn.execute(
        "SELECT table_name, row_id, op, values_json FROM winner "
        "ORDER BY table_name, row_id"
    )
    while True:
        rows = winner_cursor.fetchmany(500)
        if not rows:
            break
        for table, row_id, op, values_json in rows:
            conn.execute(
                "DELETE FROM cells WHERE table_name = ? AND row_id = ?",
                (table, row_id),
            )
            if op != "put":
                continue
            values = json.loads(values_json)
            payload = []
            for col, col_type in schema[table]:
                value_type, ival, sval, bval = _store_cell_payload(values[col])
                _assert_declared_type(col_type, value_type, table, col)
                payload.append(
                    (table, row_id, col, value_type, ival, sval, bval)
                )
            _insert_cells(conn, payload)
        conn.commit()


def _assert_declared_type(
    col_type: str, value_type: str, table: str, col: str
) -> None:
    if value_type != "null" and value_type != col_type:
        raise CorrectionLogError(
            "column %s.%s expects %s" % (table, col, col_type)
        )


def _require_keys_unique(
    conn: sqlite3.Connection,
    schema: dict[str, list[tuple[str, str]]],
) -> None:
    """Reject duplicate non-NULL lookup keys in the delivered view.

    This is a set-based check over the final state, not a replay of the base
    key constraint in file order: swaps and take-overs that end up legal are
    accepted.
    """
    for table in schema:
        key_col = conn.execute(
            "SELECT key_col FROM tables WHERE name = ?", (table,)
        ).fetchone()[0]
        if key_col is None:
            continue
        clash = conn.execute(
            "SELECT value_type, int_value, text_value, bool_value, "
            "COUNT(*), GROUP_CONCAT(row_id) FROM cells "
            "WHERE table_name = ? AND col_name = ? AND value_type != 'null' "
            "GROUP BY value_type, int_value, text_value, bool_value "
            "HAVING COUNT(*) > 1 LIMIT 1",
            (table, key_col),
        ).fetchone()
        if clash is not None:
            raise KeyConflictError(
                "duplicate key %s=%r in final table %s (rows %s)"
                % (key_col, _key_repr(clash), table, clash[5])
            )


def _key_repr(row: tuple) -> Any:
    value_type = row[0]
    if value_type == "int64":
        return row[1]
    if value_type == "text":
        return row[2]
    return bool(row[3])


def _build_snapshot(
    base: Snapshot, staging: _Staging, path: str, output_id: str
) -> Snapshot:
    """Copy schema + base rows, apply T's winners, validate the final view."""
    builder = SnapshotBuilder(path)
    schema = _copy_schema(base, builder)  # also declares key columns
    conn = builder.connection
    _copy_base_rows(base, conn, schema)
    _apply_winners(staging, conn, schema)
    _require_keys_unique(conn, schema)
    conn.commit()
    # The content digest is independent of this externally chosen name.
    return builder.finalize(snapshot_id=output_id)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def materialize_as_of(
    registry: SnapshotRegistry,
    base_id: str,
    changes_path: str,
    effective_at: int,
    known_through: int,
    output_id: str,
    *,
    scratch_root: Optional[str] = None,
) -> MaterializeResult:
    """Build, validate and publish ``output_id``; never alter ``current``."""
    _require_int64(effective_at, "effective-at")
    _require_int64(known_through, "known-through", non_negative=True)
    if not output_id or os.sep in output_id or output_id in (".", ".."):
        raise MaterializeError("invalid output id %r" % output_id)

    base = registry.open(base_id)
    base_conn = base.connect()
    try:
        base_schemas = {
            name: dict(base.schema(base_conn, name))
            for name in base.table_names(base_conn)
        }
    finally:
        base_conn.close()

    scratch = tempfile.mkdtemp(prefix="materialize-", dir=scratch_root)
    staging = _Staging(os.path.join(scratch, "changes.sqlite3"))
    try:
        count, eligible = _load_log(
            staging, changes_path, base_schemas, effective_at, known_through
        )
        winner_rows = staging.conn.execute(
            "SELECT COUNT(*) FROM winner"
        ).fetchone()[0]
        snapshot = _build_snapshot(
            base, staging, os.path.join(scratch, "snapshot"), output_id
        )
        outcome = registry.publish_without_current(snapshot)
        return MaterializeResult(
            snapshot=registry.open(output_id),
            outcome=outcome,
            effective_at=effective_at,
            known_through=known_through,
            base_snapshot_id=base_id,
            corrections_seen=count,
            corrections_eligible=eligible,
            winner_rows=int(winner_rows),
        )
    finally:
        staging.close()
        shutil.rmtree(scratch, ignore_errors=True)
