"""Deterministic replay of recorded read behaviour on a new snapshot.

The replayer never runs job code.  It walks the persisted call log in its
original order, performs the same call on the candidate snapshot through a
plain :class:`~bcache.reader.Reader`, and compares every observable:

* argument structure and call sequence
* existence / partition directory results
* lookup status, schema and every typed cell (NULL vs missing vs value)
* read schema, row order, row ids, projected cells, extra/missing rows,
  projected column type changes and negative (empty/error) results

Streaming reads are compared row by row from SQLite, so memory stays bounded
regardless of result size.  The first mismatch immediately invalidates the
candidate; no stale path is concatenated afterwards.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Optional

from .encoding import MISSING
from .errors import TableNotFound, UnknownColumn
from .reader import (
    Reader,
    _json_safe,
    hash_read_row,
    hash_read_stream,
)
from .snapshot import Snapshot


@dataclass
class ReplayReport:
    matched: bool
    reason: Optional[str] = None
    mismatched_seq: Optional[int] = None
    calls_compared: int = 0
    read_rows_compared: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "matched": self.matched,
            "reason": self.reason,
            "mismatched_seq": self.mismatched_seq,
            "calls_compared": self.calls_compared,
            "read_rows_compared": self.read_rows_compared,
            "details": self.details,
        }


def _from_json(value: Any) -> Any:
    """Reverse of reader._json_safe: restore the MISSING sentinel."""
    if isinstance(value, dict):
        if set(value.keys()) == {"__tag__"} and value["__tag__"] == "missing":
            return MISSING
        return {k: _from_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_from_json(v) for v in value]
    return value


def _predicate_from_json(value: Any) -> Optional[tuple]:
    if value is None:
        return None

    def build(node: Any) -> tuple:
        return tuple(build(child) if isinstance(child, list) else child for child in node)

    return build(value)


def observations_equal(a: Any, b: Any) -> bool:
    """Typed structural equality used by the replayer and exposed for tests.

    ``True`` differs from ``1``, ``None`` differs from :data:`MISSING`, and
    dict/list comparison is structural.  This is deliberately independent of
    Python's loose numeric coercion.
    """
    if a is MISSING or b is MISSING:
        return a is MISSING and b is MISSING
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, dict) and isinstance(b, dict):
        if set(a) != set(b):
            return False
        return all(observations_equal(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        return all(observations_equal(x, y) for x, y in zip(a, b))
    if isinstance(a, (int, str, float)) and isinstance(b, (int, str, float)):
        if type(a) is not type(b):
            return False
        return a == b
    return a == b


class ReplayValidator:
    def __init__(self, snapshot: Snapshot, store_conn: sqlite3.Connection, entry_id: str):
        self.snapshot = snapshot
        self.conn = store_conn
        self.entry_id = entry_id
        self.reader = Reader(snapshot, snapshot.connect())
        self.report = ReplayReport(matched=True)

    def close(self) -> None:
        self.reader.conn.close()

    def validate(self) -> ReplayReport:
        calls = self.conn.execute(
            "SELECT seq, method, args_json, outcome, error_type, result_digest, "
            "detail_json FROM calls WHERE entry_id = ? ORDER BY seq",
            (self.entry_id,),
        ).fetchall()
        for call in calls:
            seq = call[0]
            method = call[1]
            args = json.loads(call[2])
            recorded_outcome = call[3]
            recorded_error = call[4]
            recorded_digest = call[5]
            recorded_detail = json.loads(call[6])
            self.report.calls_compared += 1
            if method == "exists":
                mismatch = self._check_exists(seq, args, recorded_detail, recorded_digest)
            elif method == "list_partitions":
                mismatch = self._check_partitions(seq, args, recorded_detail, recorded_digest)
            elif method == "lookup":
                mismatch = self._check_lookup(seq, args, recorded_detail, recorded_outcome)
            elif method == "read":
                mismatch = self._check_read(
                    seq, args, recorded_detail, recorded_outcome, recorded_error
                )
            else:
                mismatch = "unknown recorded method %r" % method
            if mismatch is not None:
                self.report.matched = False
                self.report.reason = mismatch
                self.report.mismatched_seq = seq
                return self.report
        self.report.matched = True
        return self.report

    # -- simple observations ------------------------------------------------

    def _fail(self, seq: int, message: str, **extra: Any) -> str:
        self.report.details = {"seq": seq, **extra}
        return message

    def _check_exists(self, seq, args, detail, digest) -> Optional[str]:
        found = self.reader.exists(args["table"])
        if found != detail["exists"]:
            return self._fail(
                seq,
                "exists(%r) changed: was %s now %s"
                % (args["table"], detail["exists"], found),
                recorded=detail["exists"],
                observed=found,
            )
        return None

    def _check_partitions(self, seq, args, detail, digest) -> Optional[str]:
        names = self.reader.list_partitions(args["prefix"])
        recorded = detail["partitions"]
        if names != recorded:
            if len(names) != len(recorded):
                reason = "partition set size changed: %d -> %d" % (
                    len(recorded),
                    len(names),
                )
            else:
                reason = "partition names/order changed"
            return self._fail(
                seq,
                "list_partitions(%r): %s" % (args["prefix"], reason),
                recorded=recorded,
                observed=names,
            )
        return None

    def _check_lookup(self, seq, args, detail, outcome) -> Optional[str]:
        result = self.reader.lookup(args["table"], _from_json(args["key_value"]),
                                    args.get("key_col"))
        recorded_status = detail["status"]
        if result.status != recorded_status:
            return self._fail(
                seq,
                "lookup status changed: %s -> %s" % (recorded_status, result.status),
                recorded=recorded_status,
                observed=result.status,
            )
        recorded_schema = [(c["name"], c["type"]) for c in detail.get("schema", [])]
        if list(result.schema) != recorded_schema:
            return self._fail(
                seq,
                "lookup schema changed",
                recorded=recorded_schema,
                observed=list(result.schema),
            )
        if recorded_status == "found":
            recorded_value = _from_json(detail["value"])
            if not observations_equal(result.value, recorded_value):
                return self._fail(
                    seq,
                    "lookup value changed for key %r" % (args["key_value"],),
                    recorded=_json_safe(recorded_value),
                    observed=_json_safe(result.value),
                )
        return None

    # -- streamed read ------------------------------------------------------

    def _check_read(
        self, seq, args, detail, recorded_outcome, recorded_error
    ) -> Optional[str]:
        table = args["table"]
        columns = list(args["columns"])
        predicate_obj = _predicate_from_json(args["predicate"])
        truncated = bool(detail.get("truncated", False))
        recorded_count = int(detail["row_count"])

        if recorded_outcome == "error":
            try:
                self.reader.read_all(table, predicate_obj, columns)
            except (TableNotFound, UnknownColumn):
                return None  # same negative observation
            return self._fail(
                seq,
                "read previously failed with %s but now succeeds" % recorded_error,
                recorded_error=recorded_error,
            )

        try:
            schema, _ = self.reader._prepare_read(table, predicate_obj, columns)
        except (TableNotFound, UnknownColumn) as exc:
            return self._fail(
                seq,
                "read previously succeeded but now raises %s" % type(exc).__name__,
                observed_error=type(exc).__name__,
            )
        recorded_schema = [(c["name"], c["type"]) for c in detail["schema"]]
        if schema != recorded_schema:
            return self._fail(
                seq,
                "read schema changed (column type/set drift on projected columns)",
                recorded=recorded_schema,
                observed=schema,
            )

        hasher = hash_read_stream(schema)
        row_idx = 0
        for row in self.reader.read(table, predicate_obj, columns):
            if row_idx >= recorded_count:
                if truncated:
                    # The job abandoned the iterator here last time; rows it
                    # never consumed are not part of the observation and must
                    # not invalidate it.
                    break
                # A fully consumed read gaining an extra row invalidates,
                # even if all old rows still look identical.
                return self._fail(
                    seq,
                    "read now returns extra row_id %s at position %d "
                    "(newly matching row)" % (row.row_id, row_idx),
                    position=row_idx,
                    row_id=row.row_id,
                )
            recorded = self._recorded_row(seq, row_idx)
            rec_row_id = recorded[0]
            rec_projection = _from_json(json.loads(recorded[1]))
            if row.row_id != rec_row_id:
                return self._fail(
                    seq,
                    "read row order/identity changed at position %d: "
                    "row_id %s -> %s" % (row_idx, rec_row_id, row.row_id),
                    position=row_idx,
                )
            if not observations_equal(row.values, rec_projection):
                return self._fail(
                    seq,
                    "read row content changed at row_id %s" % row.row_id,
                    position=row_idx,
                    row_id=row.row_id,
                    recorded=_json_safe(rec_projection),
                    observed=_json_safe(row.values),
                )
            hash_read_row(hasher, row.row_id, row.values)
            row_idx += 1
            self.report.read_rows_compared += 1
            if truncated and row_idx == recorded_count:
                break

        if row_idx < recorded_count:
            return self._fail(
                seq,
                "read lost rows: recorded %d consumed rows, now %d"
                % (recorded_count, row_idx),
                recorded_count=recorded_count,
                observed_count=row_idx,
            )
        if not truncated and hasher.hexdigest() != self._recorded_digest(seq):
            return self._fail(
                seq,
                "read observation digest mismatch despite row count equality",
            )
        return None

    def _recorded_row(self, seq: int, row_idx: int) -> tuple:
        row = self.conn.execute(
            "SELECT row_id, projection_json FROM read_rows "
            "WHERE entry_id = ? AND seq = ? AND row_idx = ?",
            (self.entry_id, seq, row_idx),
        ).fetchone()
        if row is None:
            raise RuntimeError("evidence missing row %d for call %d" % (row_idx, seq))
        return row

    def _recorded_digest(self, seq: int) -> str:
        row = self.conn.execute(
            "SELECT result_digest FROM calls WHERE entry_id = ? AND seq = ?",
            (self.entry_id, seq),
        ).fetchone()
        return row[0]
