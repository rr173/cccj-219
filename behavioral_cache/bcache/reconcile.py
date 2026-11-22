"""Three-way reconcile of one lookup-keyed table across published snapshots.

Two maintenance groups revised the same repository data independently and
each published a snapshot.  ``reconcile-table`` merges one table from a
named common *base* snapshot and the two *left* / *right* revisions, keeps
every change the two sides agree on or made in different places, and asks a
reviewer to rule on the spots where they truly disagree.  Everything else
in the delivered snapshot comes from the base unchanged (including empty
tables); the other tables of the two branches are never merged.  The
operator names the common base explicitly — no ancestor search and no
branch history is involved.

Row/cell phase
--------------
Rows are aligned by ``(table, row_id)`` — never by the mutable key value.
For rows kept on both sides the merge is column by column: an identical
change on both sides is kept once, a change made by only one side is
taken, and two different new values for the same cell are a *cell
conflict* the reviewer resolves by picking a side.  Comparison is typed;
a missing cell is not the same as an explicit NULL.  A delete on one side
against an unchanged other side deletes the row; delete against modify is
a *row conflict* where picking a side means taking that side's whole row
or the deletion.  Rows absent from the base are adopted whole when only
one side added them; the same row id added by both sides with identical
content is kept once, with different content it is a row conflict (the
two new rows are never spliced together).  Row conflicts are not broken
down into cell conflicts.

Key phase
---------
Only after every row/cell conflict is ruled, the merged lookup keys are
checked as one set: a key that now sits on more than one row is listed
with every competing row id.  The reviewer assigns each participating row
its final text key (keeping the old one or naming a new one); nothing
else may change.  All key rulings take effect together and uniqueness is
judged on the final state of the whole table, so handing a key from one
row to another is fine as long as the end state has no duplicates; a
ruling that collides with an uninvolved row or with another ruling is
rejected and nothing is published.  While row/cell conflicts are still
open, intermediate key ownership is never guessed.

Viewing vs. submitting
----------------------
The same command views conflicts and submits rulings (``--decisions``).
While conflicts are pending the exit code is 3 and stdout is UTF-8 JSONL:
a header line (stage, table, base/left/right content summaries — plus a
*token* at the key stage) followed by one record per unresolved conflict
in a stable order.  The decisions file is JSONL as well: a header line
with the same table and three-input summary, then one ruling per line.
Key-stage rulings must carry the token and resubmit the very row/cell
rulings it was issued for; reordering rulings changes nothing, changing
an earlier ruling invalidates the token.  Duplicate rulings, rulings
naming no conflict, wrong types, a mismatched context, or key rulings
submitted while the row stage is unresolved are errors (exit 2).

Safety and resource bounds
--------------------------
Inputs and ``current`` are never modified.  The merged snapshot is built
and fully validated in a scratch directory and only then published under
``--output-id``; the same id with identical content is an idempotent
success, different content is refused.  All intermediate state (cells,
diffs, conflicts, rulings) lives in a temporary SQLite database, the
conflict listing streams record by record, and the final table streams
into the builder — peak Python memory stays independent of row and
conflict counts, and no source table is rescanned per ruling.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from typing import Any, BinaryIO, Optional

from .encoding import MISSING, StreamHasher
from .errors import SnapshotConflict  # noqa: F401  (re-exported for callers)
from .snapshot import Snapshot, SnapshotBuilder, SnapshotRegistry, _decode_cell

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

_CELL_COLS = "row_id, col_name, value_type, int_value, text_value, bool_value"
_INSERT_CELL_SQL = (
    "INSERT INTO cells(table_name, row_id, col_name, value_type, "
    "int_value, text_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?)"
)


class ReconcileError(ValueError):
    """A reconcile request that cannot be satisfied as stated (exit 2)."""


class ReconcileInputError(ReconcileError):
    """The named snapshots or table fail the reconcile preconditions."""


class ReconcileDecisionError(ReconcileError):
    """The decisions file is malformed, mismatched or illegal."""


class PendingDecisions(Exception):
    """Conflicts await rulings; the JSONL listing was written to the stream."""

    def __init__(self, stage: str):
        super().__init__("reconcile pending %s-stage rulings" % stage)
        self.stage = stage


@dataclass(frozen=True)
class ReconcileResult:
    snapshot: Snapshot
    outcome: str  # "published" | "unchanged"
    table: str
    base_id: str
    left_id: str
    right_id: str
    rows: int
    row_conflicts: int
    cell_conflicts: int
    key_conflicts: int
    decisions_applied: int


# ---------------------------------------------------------------------------
# Staging database
# ---------------------------------------------------------------------------

_STAGING_SQL = """
CREATE TABLE cells(
    src TEXT NOT NULL,              -- 'base' | 'left' | 'right'
    row_id INTEGER NOT NULL,
    col_name TEXT NOT NULL,
    value_type TEXT NOT NULL,
    int_value INTEGER,
    text_value TEXT,
    bool_value INTEGER
);
CREATE INDEX cells_idx ON cells(src, row_id, col_name);

CREATE TABLE wide(
    row_id INTEGER PRIMARY KEY,
    in_base INTEGER NOT NULL,
    in_left INTEGER NOT NULL,
    in_right INTEGER NOT NULL
);

-- Cell-level differences between a pair of sides (either direction,
-- including cells missing on one side).
CREATE TABLE diff_bl(row_id INTEGER NOT NULL, col_name TEXT NOT NULL,
                     PRIMARY KEY(row_id, col_name));
CREATE TABLE diff_br(row_id INTEGER NOT NULL, col_name TEXT NOT NULL,
                     PRIMARY KEY(row_id, col_name));
CREATE TABLE diff_lr(row_id INTEGER NOT NULL, col_name TEXT NOT NULL,
                     PRIMARY KEY(row_id, col_name));
CREATE TABLE rdiff_bl(row_id INTEGER PRIMARY KEY);
CREATE TABLE rdiff_br(row_id INTEGER PRIMARY KEY);
CREATE TABLE rdiff_lr(row_id INTEGER PRIMARY KEY);

CREATE TABLE schema_cols(col_name TEXT PRIMARY KEY, ordinal INTEGER NOT NULL);

-- action: 'base' | 'left' | 'right' | 'drop' | 'merge' | 'conflict'
CREATE TABLE row_class(row_id INTEGER PRIMARY KEY, action TEXT NOT NULL);
-- kind: 'delete_left' | 'delete_right' | 'add_both'
CREATE TABLE conflict_row(row_id INTEGER PRIMARY KEY, kind TEXT NOT NULL);

CREATE TABLE cell_merge(row_id INTEGER NOT NULL, col_name TEXT NOT NULL,
                        src TEXT NOT NULL, PRIMARY KEY(row_id, col_name));
CREATE TABLE cell_result(row_id INTEGER NOT NULL, col_name TEXT NOT NULL,
                         src TEXT NOT NULL, PRIMARY KEY(row_id, col_name));
CREATE TABLE conflict_cell(row_id INTEGER NOT NULL, col_name TEXT NOT NULL,
                           PRIMARY KEY(row_id, col_name));

CREATE TABLE decision_row(row_id INTEGER PRIMARY KEY, side TEXT NOT NULL);
CREATE TABLE decision_cell(row_id INTEGER NOT NULL, col_name TEXT NOT NULL,
                           side TEXT NOT NULL, PRIMARY KEY(row_id, col_name));
CREATE TABLE decision_key(row_id INTEGER PRIMARY KEY, key_text TEXT NOT NULL);

CREATE TABLE final_cells(
    row_id INTEGER NOT NULL,
    col_name TEXT NOT NULL,
    value_type TEXT NOT NULL,
    int_value INTEGER,
    text_value TEXT,
    bool_value INTEGER
);
CREATE INDEX final_cells_idx ON final_cells(row_id, col_name);

CREATE TABLE merged_keys(row_id INTEGER PRIMARY KEY, key_text TEXT NOT NULL);
CREATE INDEX merged_keys_key ON merged_keys(key_text);
CREATE TABLE key_conflict(key_text TEXT NOT NULL, row_id INTEGER NOT NULL,
                          PRIMARY KEY(key_text, row_id));
"""


def _diff_sql(a: str, b: str) -> str:
    """(row_id, col) pairs where side ``a`` and side ``b`` differ.

    EXCEPT compares with NULLs considered equal, so typed cells compare
    exactly; a cell missing on one side simply has no row there.
    """
    return (
        "SELECT row_id, col_name FROM ("
        "SELECT %s FROM cells WHERE src = '%s' EXCEPT "
        "SELECT %s FROM cells WHERE src = '%s') "
        "UNION "
        "SELECT row_id, col_name FROM ("
        "SELECT %s FROM cells WHERE src = '%s' EXCEPT "
        "SELECT %s FROM cells WHERE src = '%s')"
        % (_CELL_COLS, a, _CELL_COLS, b, _CELL_COLS, b, _CELL_COLS, a)
    )


_ROW_CLASS_SQL = """
INSERT INTO row_class
SELECT w.row_id,
  CASE
    WHEN w.in_base = 1 AND w.in_left = 1 AND w.in_right = 1 THEN
      CASE
        WHEN bl.row_id IS NULL AND br.row_id IS NULL THEN 'base'
        WHEN bl.row_id IS NOT NULL AND br.row_id IS NULL THEN 'left'
        WHEN bl.row_id IS NULL AND br.row_id IS NOT NULL THEN 'right'
        WHEN lr.row_id IS NULL THEN 'left'  -- both changed identically
        ELSE 'merge'
      END
    WHEN w.in_base = 1 AND w.in_left = 1 THEN   -- right deleted the row
      CASE WHEN bl.row_id IS NULL THEN 'drop' ELSE 'conflict' END
    WHEN w.in_base = 1 AND w.in_right = 1 THEN  -- left deleted the row
      CASE WHEN br.row_id IS NULL THEN 'drop' ELSE 'conflict' END
    WHEN w.in_base = 1 THEN 'drop'              -- both deleted the row
    WHEN w.in_left = 1 AND w.in_right = 1 THEN  -- added on both sides
      CASE WHEN lr.row_id IS NULL THEN 'left' ELSE 'conflict' END
    WHEN w.in_left = 1 THEN 'left'              -- added by left only
    ELSE 'right'                                -- added by right only
  END
FROM wide w
LEFT JOIN rdiff_bl bl ON bl.row_id = w.row_id
LEFT JOIN rdiff_br br ON br.row_id = w.row_id
LEFT JOIN rdiff_lr lr ON lr.row_id = w.row_id
"""

_CONFLICT_ROW_SQL = """
INSERT INTO conflict_row
SELECT w.row_id,
  CASE
    WHEN w.in_left = 1 AND w.in_right = 0 THEN 'delete_right'
    WHEN w.in_left = 0 AND w.in_right = 1 THEN 'delete_left'
    ELSE 'add_both'
  END
FROM wide w
WHERE w.row_id IN (SELECT row_id FROM row_class WHERE action = 'conflict')
"""

# For rows present in all three snapshots and changed on both sides:
# membership in the three pairwise cell diffs decides each column.
# (bl, br, lr) = (1,1,1) is a true cell conflict; (1,1,0) both changed
# identically; (1,0,1) only left changed; (0,1,1) only right changed;
# (0,0,0) unchanged.  The remaining patterns cannot occur and are kept
# as conflicts defensively.
_CELL_MERGE_SQL = """
INSERT INTO cell_merge
SELECT m.row_id, s.col_name,
  CASE
    WHEN bl.row_id IS NOT NULL AND br.row_id IS NOT NULL
         AND lr.row_id IS NOT NULL THEN 'conflict'
    WHEN bl.row_id IS NOT NULL AND br.row_id IS NOT NULL THEN 'left'
    WHEN bl.row_id IS NOT NULL AND lr.row_id IS NOT NULL THEN 'left'
    WHEN br.row_id IS NOT NULL AND lr.row_id IS NOT NULL THEN 'right'
    WHEN bl.row_id IS NULL AND br.row_id IS NULL
         AND lr.row_id IS NULL THEN 'base'
    ELSE 'conflict'
  END
FROM (SELECT row_id FROM row_class WHERE action = 'merge') m
CROSS JOIN schema_cols s
LEFT JOIN diff_bl bl ON bl.row_id = m.row_id AND bl.col_name = s.col_name
LEFT JOIN diff_br br ON br.row_id = m.row_id AND br.col_name = s.col_name
LEFT JOIN diff_lr lr ON lr.row_id = m.row_id AND lr.col_name = s.col_name
"""

_APPLY_ROW_DECISIONS_SQL = """
UPDATE row_class SET action = (
  SELECT CASE cr.kind
    WHEN 'delete_right' THEN CASE d.side WHEN 'left' THEN 'left' ELSE 'drop' END
    WHEN 'delete_left' THEN CASE d.side WHEN 'right' THEN 'right' ELSE 'drop' END
    ELSE d.side
  END
  FROM conflict_row cr JOIN decision_row d ON d.row_id = cr.row_id
  WHERE cr.row_id = row_class.row_id
)
WHERE row_id IN (SELECT row_id FROM decision_row)
"""

_UNRESOLVED_ROW_CONFLICTS_SQL = """
SELECT row_id, NULL AS col_name, 'row' AS kind FROM conflict_row cr
WHERE NOT EXISTS (SELECT 1 FROM decision_row d WHERE d.row_id = cr.row_id)
UNION ALL
SELECT row_id, col_name, 'cell' FROM conflict_cell cc
WHERE NOT EXISTS (SELECT 1 FROM decision_cell d
                  WHERE d.row_id = cc.row_id AND d.col_name = cc.col_name)
ORDER BY row_id, col_name
"""

_UNRESOLVED_KEY_CONFLICTS_SQL = """
SELECT kc.key_text, kc.row_id FROM key_conflict kc
WHERE EXISTS (
  SELECT 1 FROM key_conflict k2
  WHERE k2.key_text = kc.key_text
    AND NOT EXISTS (SELECT 1 FROM decision_key d WHERE d.row_id = k2.row_id))
ORDER BY kc.key_text, kc.row_id
"""


# ---------------------------------------------------------------------------
# Input loading and preconditions
# ---------------------------------------------------------------------------

def _load_inputs(
    staging: sqlite3.Connection,
    snapshots: dict[str, Snapshot],
    table: str,
) -> None:
    """Stream the reconciled table's cells from each snapshot into staging."""
    for side in ("base", "left", "right"):
        conn = snapshots[side].connect()
        try:
            cursor = conn.execute(
                "SELECT " + _CELL_COLS + " FROM cells WHERE table_name = ? "
                "ORDER BY row_id, col_name",
                (table,),
            )
            batch: list[tuple] = []
            for raw in cursor:
                batch.append((side, *raw))
                if len(batch) >= 1000:
                    staging.executemany(
                        "INSERT INTO cells VALUES (?, ?, ?, ?, ?, ?, ?)", batch
                    )
                    batch.clear()
            if batch:
                staging.executemany(
                    "INSERT INTO cells VALUES (?, ?, ?, ?, ?, ?, ?)", batch
                )
        finally:
            conn.close()
    staging.commit()


def _check_key_preconditions(staging: sqlite3.Connection, key_col: str) -> None:
    """Every existing row needs a non-NULL key; keys unique within an input."""
    bad = staging.execute(
        "SELECT src, row_id FROM (SELECT DISTINCT src, row_id FROM cells) p "
        "WHERE NOT EXISTS (SELECT 1 FROM cells k "
        "  WHERE k.src = p.src AND k.row_id = p.row_id "
        "  AND k.col_name = ? AND k.value_type = 'text') LIMIT 5",
        (key_col,),
    ).fetchall()
    if bad:
        detail = ", ".join("%s row_id=%r" % (src, rid) for src, rid in bad)
        raise ReconcileInputError(
            "every row must carry a non-NULL text key %r; violations: %s"
            % (key_col, detail)
        )
    dup = staging.execute(
        "SELECT src, text_value, COUNT(*) AS c FROM cells "
        "WHERE col_name = ? AND value_type = 'text' "
        "GROUP BY src, text_value HAVING c > 1 LIMIT 5",
        (key_col,),
    ).fetchall()
    if dup:
        detail = ", ".join(
            "%s key %r on %d rows" % (src, key, c) for src, key, c in dup
        )
        raise ReconcileInputError(
            "duplicate lookup keys within one input: %s" % detail
        )


# ---------------------------------------------------------------------------
# Row/cell phase computation
# ---------------------------------------------------------------------------

def _compute_row_phase(
    staging: sqlite3.Connection, schema: list[tuple[str, str]]
) -> None:
    staging.executemany(
        "INSERT INTO schema_cols(col_name, ordinal) VALUES (?, ?)",
        [(col, i) for i, (col, _t) in enumerate(schema)],
    )
    staging.execute(
        "INSERT INTO wide "
        "SELECT row_id, "
        "MAX(CASE WHEN src = 'base' THEN 1 ELSE 0 END), "
        "MAX(CASE WHEN src = 'left' THEN 1 ELSE 0 END), "
        "MAX(CASE WHEN src = 'right' THEN 1 ELSE 0 END) "
        "FROM (SELECT DISTINCT src, row_id FROM cells) GROUP BY row_id"
    )
    for name, a, b in (
        ("bl", "base", "left"),
        ("br", "base", "right"),
        ("lr", "left", "right"),
    ):
        staging.execute("INSERT INTO diff_%s %s" % (name, _diff_sql(a, b)))
        staging.execute(
            "INSERT INTO rdiff_%s SELECT DISTINCT row_id FROM diff_%s"
            % (name, name)
        )
    staging.execute(_ROW_CLASS_SQL)
    staging.execute(_CONFLICT_ROW_SQL)
    staging.execute(_CELL_MERGE_SQL)
    staging.execute(
        "INSERT INTO cell_result SELECT row_id, col_name, src "
        "FROM cell_merge WHERE src != 'conflict'"
    )
    staging.execute(
        "INSERT INTO conflict_cell SELECT row_id, col_name "
        "FROM cell_merge WHERE src = 'conflict'"
    )
    staging.commit()


def _apply_row_decisions(staging: sqlite3.Connection) -> None:
    staging.execute(_APPLY_ROW_DECISIONS_SQL)
    staging.execute(
        "INSERT INTO cell_result SELECT row_id, col_name, side "
        "FROM decision_cell"
    )
    staging.commit()


def _build_final_cells(staging: sqlite3.Connection) -> None:
    staging.execute(
        "INSERT INTO final_cells "
        "SELECT c.row_id, c.col_name, c.value_type, c.int_value, "
        "c.text_value, c.bool_value "
        "FROM row_class rc JOIN cells c "
        "  ON c.row_id = rc.row_id AND c.src = rc.action "
        "WHERE rc.action IN ('base', 'left', 'right')"
    )
    staging.execute(
        "INSERT INTO final_cells "
        "SELECT cr.row_id, cr.col_name, c.value_type, c.int_value, "
        "c.text_value, c.bool_value "
        "FROM cell_result cr JOIN cells c "
        "  ON c.row_id = cr.row_id AND c.col_name = cr.col_name "
        "  AND c.src = cr.src"
    )
    staging.commit()


# ---------------------------------------------------------------------------
# Key phase
# ---------------------------------------------------------------------------

def _compute_key_conflicts(staging: sqlite3.Connection, key_col: str) -> None:
    staging.execute(
        "INSERT INTO merged_keys SELECT row_id, text_value FROM final_cells "
        "WHERE col_name = ?",
        (key_col,),
    )
    staging.execute(
        "INSERT INTO key_conflict SELECT key_text, row_id FROM merged_keys "
        "WHERE key_text IN (SELECT key_text FROM merged_keys "
        "GROUP BY key_text HAVING COUNT(*) > 1)"
    )
    staging.commit()


def _apply_key_decisions(staging: sqlite3.Connection, key_col: str) -> None:
    staging.execute(
        "UPDATE final_cells SET text_value = ("
        "  SELECT key_text FROM decision_key "
        "  WHERE decision_key.row_id = final_cells.row_id) "
        "WHERE col_name = ? AND row_id IN (SELECT row_id FROM decision_key)",
        (key_col,),
    )
    staging.commit()


def _check_final_keys(staging: sqlite3.Connection, key_col: str) -> None:
    """Uniqueness is judged on the final state of the whole result table."""
    bad_type = staging.execute(
        "SELECT row_id FROM final_cells WHERE col_name = ? "
        "AND value_type != 'text' LIMIT 1",
        (key_col,),
    ).fetchone()
    if bad_type is not None:
        raise ReconcileError(
            "internal error: merged row_id=%r lost its text key" % bad_type[0]
        )
    clashes = staging.execute(
        "SELECT text_value, row_id FROM final_cells "
        "WHERE col_name = ? AND text_value IN ("
        "  SELECT text_value FROM final_cells WHERE col_name = ? "
        "  GROUP BY text_value HAVING COUNT(*) > 1) "
        "ORDER BY text_value, row_id LIMIT 10",
        (key_col, key_col),
    ).fetchall()
    if clashes:
        by_key: dict[str, list[int]] = {}
        for key_text, row_id in clashes:
            by_key.setdefault(key_text, []).append(row_id)
        detail = "; ".join(
            "key %r on rows %s" % (k, v) for k, v in sorted(by_key.items())
        )
        raise ReconcileDecisionError(
            "key rulings leave duplicate keys in the final table "
            "(collision with an uninvolved row or another ruling); "
            "not publishing: %s" % detail
        )


# ---------------------------------------------------------------------------
# Decisions file
# ---------------------------------------------------------------------------

def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise ReconcileDecisionError("duplicate JSON member %r" % key)
        obj[key] = value
    return obj


def _reject_constant(name: str) -> Any:
    raise ReconcileDecisionError("non-JSON constant %s is not allowed" % name)


def _parse_json_object(line: bytes, lineno: int) -> dict[str, Any]:
    try:
        text = line.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReconcileDecisionError(
            "line %d is not valid UTF-8" % lineno
        ) from exc
    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_constant,
        )
    except json.JSONDecodeError as exc:
        raise ReconcileDecisionError(
            "line %d is not valid JSON: %s" % (lineno, exc)
        ) from exc
    if not isinstance(parsed, dict):
        raise ReconcileDecisionError("line %d must be a JSON object" % lineno)
    return parsed


def _exact_fields(obj: dict[str, Any], allowed: set[str], where: str) -> None:
    missing = allowed - set(obj)
    if missing:
        raise ReconcileDecisionError(
            "%s: missing fields %s" % (where, sorted(missing))
        )
    unknown = set(obj) - allowed
    if unknown:
        raise ReconcileDecisionError(
            "%s: unknown fields %s" % (where, sorted(unknown))
        )


def _int64_field(value: Any, where: str, name: str) -> int:
    if not _is_int(value):
        raise ReconcileDecisionError(
            "%s: %s must be an int64 integer, got %s"
            % (where, name, type(value).__name__)
        )
    if not (INT64_MIN <= value <= INT64_MAX):
        raise ReconcileDecisionError(
            "%s: %s is outside the signed 64-bit range" % (where, name)
        )
    return value


def _side_field(value: Any, where: str) -> str:
    if value not in ("left", "right"):
        raise ReconcileDecisionError(
            "%s: side must be 'left' or 'right', got %r" % (where, value)
        )
    return value


def _insert_decision(
    staging: sqlite3.Connection, sql: str, params: tuple, duplicate_msg: str
) -> None:
    try:
        staging.execute(sql, params)
    except sqlite3.IntegrityError as exc:
        raise ReconcileDecisionError(duplicate_msg) from exc


def _check_header(
    obj: dict[str, Any], table: str, inputs: dict[str, Any], lineno: int
) -> Optional[str]:
    where = "line %d" % lineno
    if obj.get("kind") != "header":
        raise ReconcileDecisionError(
            "%s: the first record must be a header" % where
        )
    unknown = set(obj) - {"kind", "table", "inputs", "token", "stage"}
    if unknown:
        raise ReconcileDecisionError(
            "%s: unknown header fields %s" % (where, sorted(unknown))
        )
    if obj.get("table") != table:
        raise ReconcileDecisionError(
            "%s: header table %r does not match --table %r"
            % (where, obj.get("table"), table)
        )
    if obj.get("inputs") != inputs:
        raise ReconcileDecisionError(
            "%s: header inputs do not match the three named snapshots" % where
        )
    token = obj.get("token")
    if token is not None and not isinstance(token, str):
        raise ReconcileDecisionError("%s: token must be a string" % where)
    return token


def _parse_decision_line(
    obj: dict[str, Any], lineno: int, staging: sqlite3.Connection
) -> None:
    where = "line %d" % lineno
    if obj.get("kind") != "decision":
        raise ReconcileDecisionError(
            "%s: kind must be 'decision'" % where
        )
    conflict = obj.get("conflict")
    if conflict == "row":
        _exact_fields(obj, {"kind", "conflict", "row_id", "side"}, where)
        row_id = _int64_field(obj["row_id"], where, "row_id")
        side = _side_field(obj["side"], where)
        _insert_decision(
            staging,
            "INSERT INTO decision_row(row_id, side) VALUES (?, ?)",
            (row_id, side),
            "%s: duplicate ruling for the row conflict on row_id %r"
            % (where, row_id),
        )
    elif conflict == "cell":
        _exact_fields(
            obj, {"kind", "conflict", "row_id", "column", "side"}, where
        )
        row_id = _int64_field(obj["row_id"], where, "row_id")
        column = obj["column"]
        if not isinstance(column, str) or not column:
            raise ReconcileDecisionError(
                "%s: column must be a non-empty string" % where
            )
        side = _side_field(obj["side"], where)
        _insert_decision(
            staging,
            "INSERT INTO decision_cell(row_id, col_name, side) "
            "VALUES (?, ?, ?)",
            (row_id, column, side),
            "%s: duplicate ruling for the cell conflict on row_id %r "
            "column %r" % (where, row_id, column),
        )
    elif conflict == "key":
        _exact_fields(obj, {"kind", "conflict", "row_id", "key"}, where)
        row_id = _int64_field(obj["row_id"], where, "row_id")
        key = obj["key"]
        if not isinstance(key, str):
            raise ReconcileDecisionError(
                "%s: key must be a text string" % where
            )
        _insert_decision(
            staging,
            "INSERT INTO decision_key(row_id, key_text) VALUES (?, ?)",
            (row_id, key),
            "%s: duplicate key ruling for row_id %r" % (where, row_id),
        )
    else:
        raise ReconcileDecisionError(
            "%s: unknown conflict kind %r" % (where, conflict)
        )


def _load_decisions(
    staging: sqlite3.Connection,
    path: Optional[str],
    table: str,
    inputs: dict[str, Any],
) -> Optional[str]:
    """Parse and structurally validate the decisions file; return its token."""
    if path is None:
        return None
    try:
        fh = open(path, "rb")
    except OSError as exc:
        raise ReconcileDecisionError(
            "cannot read decisions file %s: %s" % (path, exc)
        ) from exc
    token: Optional[str] = None
    with fh:
        header_seen = False
        for lineno, raw in enumerate(fh, start=1):
            if not raw.strip():
                continue
            obj = _parse_json_object(raw, lineno)
            if not header_seen:
                token = _check_header(obj, table, inputs, lineno)
                header_seen = True
            else:
                _parse_decision_line(obj, lineno, staging)
        if not header_seen:
            raise ReconcileDecisionError("decisions file %s is empty" % path)
    staging.commit()
    return token


def _check_row_decision_references(staging: sqlite3.Connection) -> None:
    row = staging.execute(
        "SELECT row_id FROM decision_row d WHERE NOT EXISTS "
        "(SELECT 1 FROM conflict_row c WHERE c.row_id = d.row_id) LIMIT 1"
    ).fetchone()
    if row is not None:
        raise ReconcileDecisionError(
            "ruling for row_id %r names no row conflict" % (row[0],)
        )
    row = staging.execute(
        "SELECT row_id, col_name FROM decision_cell d WHERE NOT EXISTS "
        "(SELECT 1 FROM conflict_cell c WHERE c.row_id = d.row_id "
        "AND c.col_name = d.col_name) LIMIT 1"
    ).fetchone()
    if row is not None:
        raise ReconcileDecisionError(
            "ruling for row_id %r column %r names no cell conflict" % tuple(row)
        )


def _check_key_decision_references(staging: sqlite3.Connection) -> None:
    row = staging.execute(
        "SELECT row_id FROM decision_key d WHERE NOT EXISTS "
        "(SELECT 1 FROM key_conflict k WHERE k.row_id = d.row_id) LIMIT 1"
    ).fetchone()
    if row is not None:
        raise ReconcileDecisionError(
            "key ruling for row_id %r names no contested key" % (row[0],)
        )


# ---------------------------------------------------------------------------
# Token binding inputs + adopted row/cell rulings
# ---------------------------------------------------------------------------

def _compute_token(
    staging: sqlite3.Connection, table: str, inputs: dict[str, Any]
) -> str:
    hasher = StreamHasher(namespace="reconcile-key-token-v1")
    hasher.update_value(table)
    for side in ("base", "left", "right"):
        hasher.update_value(inputs[side]["id"])
        hasher.update_value(inputs[side]["digest"])
    for row_id, side in staging.execute(
        "SELECT row_id, side FROM decision_row ORDER BY row_id"
    ):
        hasher.update_value("row")
        hasher.update_value(row_id)
        hasher.update_value(side)
    for row_id, col_name, side in staging.execute(
        "SELECT row_id, col_name, side FROM decision_cell "
        "ORDER BY row_id, col_name"
    ):
        hasher.update_value("cell")
        hasher.update_value(row_id)
        hasher.update_value(col_name)
        hasher.update_value(side)
    return hasher.hexdigest()


# ---------------------------------------------------------------------------
# Pending-conflict listing (streaming JSONL)
# ---------------------------------------------------------------------------

def _write_jsonl(out: BinaryIO, obj: Any) -> None:
    out.write(
        json.dumps(obj, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")).encode("utf-8")
        + b"\n"
    )


def _cell_state_json(value: Any) -> dict[str, Any]:
    if value is MISSING:
        return {"state": "missing"}
    if value is None:
        return {"state": "null"}
    return {"state": "value", "value": value}


def _side_row_json(
    staging: sqlite3.Connection,
    src: str,
    row_id: int,
    schema: list[tuple[str, str]],
) -> dict[str, Any]:
    found: dict[str, Any] = {}
    for col_name, vtype, ival, tval, bval in staging.execute(
        "SELECT col_name, value_type, int_value, text_value, bool_value "
        "FROM cells WHERE src = ? AND row_id = ?",
        (src, row_id),
    ):
        found[col_name] = _decode_cell(vtype, ival, tval, bval)
    if not found:
        return {"state": "absent"}
    cells = {
        col: _cell_state_json(found.get(col, MISSING)) for col, _ in schema
    }
    return {"state": "present", "cells": cells}


def _side_cell_json(
    staging: sqlite3.Connection, src: str, row_id: int, col_name: str
) -> dict[str, Any]:
    row = staging.execute(
        "SELECT value_type, int_value, text_value, bool_value FROM cells "
        "WHERE src = ? AND row_id = ? AND col_name = ?",
        (src, row_id, col_name),
    ).fetchone()
    if row is None:
        return {"state": "missing"}
    return _cell_state_json(_decode_cell(*row))


def _emit_rows_stage(
    staging: sqlite3.Connection,
    out: BinaryIO,
    table: str,
    inputs: dict[str, Any],
    schema: list[tuple[str, str]],
) -> None:
    _write_jsonl(
        out,
        {"kind": "header", "stage": "rows", "table": table, "inputs": inputs},
    )
    for row_id, col_name, kind in staging.execute(_UNRESOLVED_ROW_CONFLICTS_SQL):
        if kind == "row":
            record = {
                "kind": "conflict",
                "conflict": "row",
                "row_id": row_id,
                "base": _side_row_json(staging, "base", row_id, schema),
                "left": _side_row_json(staging, "left", row_id, schema),
                "right": _side_row_json(staging, "right", row_id, schema),
            }
        else:
            record = {
                "kind": "conflict",
                "conflict": "cell",
                "row_id": row_id,
                "column": col_name,
                "base": _side_cell_json(staging, "base", row_id, col_name),
                "left": _side_cell_json(staging, "left", row_id, col_name),
                "right": _side_cell_json(staging, "right", row_id, col_name),
            }
        _write_jsonl(out, record)


def _emit_keys_stage(
    staging: sqlite3.Connection,
    out: BinaryIO,
    table: str,
    inputs: dict[str, Any],
    token: str,
) -> None:
    _write_jsonl(
        out,
        {
            "kind": "header",
            "stage": "keys",
            "table": table,
            "inputs": inputs,
            "token": token,
        },
    )
    for key_text, row_id in staging.execute(_UNRESOLVED_KEY_CONFLICTS_SQL):
        _write_jsonl(
            out,
            {"kind": "conflict", "conflict": "key",
             "key": key_text, "row_id": row_id},
        )


# ---------------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------------

def _pump_cells(
    conn: sqlite3.Connection, table: str, cursor: Any
) -> None:
    batch: list[tuple] = []
    for raw in cursor:
        batch.append((table, *raw))
        if len(batch) >= 1000:
            conn.executemany(_INSERT_CELL_SQL, batch)
            batch.clear()
    if batch:
        conn.executemany(_INSERT_CELL_SQL, batch)


def _build_snapshot(
    base: Snapshot,
    staging: sqlite3.Connection,
    table: str,
    key_col: str,
    output_id: str,
    scratch: str,
) -> Snapshot:
    """Copy the base schema and untouched tables, stream the merged table."""
    builder = SnapshotBuilder(os.path.join(scratch, "snapshot"))
    conn = builder.connection
    base_conn = base.connect()
    try:
        names = base.table_names(base_conn)
        for name in names:
            builder.add_table(
                name,
                base.schema(base_conn, name),
                key_col=base.key_col(base_conn, name),
            )
        for name in names:
            if name == table:
                continue
            cursor = base_conn.execute(
                "SELECT " + _CELL_COLS + " FROM cells WHERE table_name = ? "
                "ORDER BY row_id, col_name",
                (name,),
            )
            _pump_cells(conn, name, cursor)
    finally:
        base_conn.close()
    cursor = staging.execute(
        "SELECT " + _CELL_COLS + " FROM final_cells ORDER BY row_id, col_name"
    )
    _pump_cells(conn, table, cursor)
    conn.commit()
    clash = conn.execute(
        "SELECT text_value, COUNT(*) AS c FROM cells "
        "WHERE table_name = ? AND col_name = ? AND value_type = 'text' "
        "GROUP BY text_value HAVING c > 1 LIMIT 1",
        (table, key_col),
    ).fetchone()
    if clash is not None:
        raise ReconcileError(
            "internal error: duplicate key %r in the built table %r"
            % (clash[0], table)
        )
    return builder.finalize(snapshot_id=output_id)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def _count(staging: sqlite3.Connection, table_name: str) -> int:
    return int(
        staging.execute("SELECT COUNT(*) FROM %s" % table_name).fetchone()[0]
    )


def reconcile_table(
    registry: SnapshotRegistry,
    base_id: str,
    left_id: str,
    right_id: str,
    table: str,
    output_id: str,
    *,
    decisions_path: Optional[str] = None,
    pending_out: Optional[BinaryIO] = None,
    scratch_root: Optional[str] = None,
) -> ReconcileResult:
    """Merge ``table`` across base/left/right and publish ``output_id``.

    Never alters the inputs or ``current``.  Raises
    :class:`PendingDecisions` (after streaming the JSONL listing to
    ``pending_out``) while conflicts await rulings,
    :class:`ReconcileError` for illegal requests or rulings, and
    :class:`SnapshotConflict` when the output id holds different content.
    """
    if not table or not isinstance(table, str):
        raise ReconcileError("invalid table name %r" % (table,))
    if not output_id or os.sep in output_id or output_id in (".", ".."):
        raise ReconcileError("invalid output id %r" % (output_id,))

    snapshots: dict[str, Snapshot] = {}
    for side, sid in (("base", base_id), ("left", left_id), ("right", right_id)):
        try:
            snapshots[side] = registry.open(sid)
        except FileNotFoundError as exc:
            raise ReconcileInputError(
                "unknown %s snapshot %s" % (side, sid)
            ) from exc

    schema: Optional[list[tuple[str, str]]] = None
    key_col: Optional[str] = None
    inputs: dict[str, Any] = {}
    for side in ("base", "left", "right"):
        snap = snapshots[side]
        conn = snap.connect()
        try:
            if not snap.has_table(conn, table):
                raise ReconcileInputError(
                    "table %r not found in %s snapshot %s"
                    % (table, side, snap.snapshot_id)
                )
            side_schema = snap.schema(conn, table)
            side_key = snap.key_col(conn, table)
            rows = snap.count_rows(conn, table)
        finally:
            conn.close()
        if schema is None:
            schema, key_col = side_schema, side_key
        else:
            if side_schema != schema:
                raise ReconcileInputError(
                    "table %r column declaration differs in %s snapshot %s"
                    % (table, side, snap.snapshot_id)
                )
            if side_key != key_col:
                raise ReconcileInputError(
                    "table %r key declaration differs in %s snapshot %s"
                    % (table, side, snap.snapshot_id)
                )
        inputs[side] = {
            "id": snap.snapshot_id,
            "digest": snap.digest,
            "rows": rows,
        }
    assert schema is not None
    if key_col is None:
        raise ReconcileInputError(
            "table %r declares no lookup key; reconcile-table requires a "
            "text lookup key" % table
        )
    if dict(schema)[key_col] != "text":
        raise ReconcileInputError(
            "table %r lookup key %r must be declared text" % (table, key_col)
        )

    out = pending_out if pending_out is not None else sys.stdout.buffer
    scratch = tempfile.mkdtemp(prefix="reconcile-", dir=scratch_root)
    try:
        staging = sqlite3.connect(os.path.join(scratch, "staging.sqlite3"))
        try:
            staging.executescript(_STAGING_SQL)
            _load_inputs(staging, snapshots, table)
            _check_key_preconditions(staging, key_col)
            _compute_row_phase(staging, schema)
            token_submitted = _load_decisions(
                staging, decisions_path, table, inputs
            )
            _check_row_decision_references(staging)
            _apply_row_decisions(staging)
            row_conflicts = _count(staging, "conflict_row")
            cell_conflicts = _count(staging, "conflict_cell")
            key_stage_attempted = (
                token_submitted is not None
                or _count(staging, "decision_key") > 0
            )
            unresolved_rows = staging.execute(
                "SELECT 1 FROM conflict_row cr WHERE NOT EXISTS "
                "(SELECT 1 FROM decision_row d WHERE d.row_id = cr.row_id) "
                "LIMIT 1"
            ).fetchone() is not None
            if not unresolved_rows:
                unresolved_rows = staging.execute(
                    "SELECT 1 FROM conflict_cell cc WHERE NOT EXISTS "
                    "(SELECT 1 FROM decision_cell d "
                    "WHERE d.row_id = cc.row_id AND d.col_name = cc.col_name) "
                    "LIMIT 1"
                ).fetchone() is not None
            if unresolved_rows:
                if key_stage_attempted:
                    raise ReconcileDecisionError(
                        "key-stage rulings submitted while row/cell "
                        "conflicts are still unresolved"
                    )
                _emit_rows_stage(staging, out, table, inputs, schema)
                raise PendingDecisions("rows")

            _build_final_cells(staging)
            _compute_key_conflicts(staging, key_col)
            if key_stage_attempted:
                expected = _compute_token(staging, table, inputs)
                if token_submitted != expected:
                    raise ReconcileDecisionError(
                        "token does not match these inputs and the submitted "
                        "row/cell rulings; re-view the key stage and use the "
                        "token it issues"
                    )
            _check_key_decision_references(staging)
            key_conflicts = int(
                staging.execute(
                    "SELECT COUNT(DISTINCT key_text) FROM key_conflict"
                ).fetchone()[0]
            )
            unresolved_keys = staging.execute(
                "SELECT 1 FROM key_conflict kc WHERE NOT EXISTS "
                "(SELECT 1 FROM decision_key d WHERE d.row_id = kc.row_id) "
                "LIMIT 1"
            ).fetchone() is not None
            if unresolved_keys:
                token = _compute_token(staging, table, inputs)
                _emit_keys_stage(staging, out, table, inputs, token)
                raise PendingDecisions("keys")

            _apply_key_decisions(staging, key_col)
            _check_final_keys(staging, key_col)
            snapshot = _build_snapshot(
                snapshots["base"], staging, table, key_col, output_id, scratch
            )
            outcome = registry.publish_without_current(snapshot)
            return ReconcileResult(
                snapshot=registry.open(output_id),
                outcome=outcome,
                table=table,
                base_id=base_id,
                left_id=left_id,
                right_id=right_id,
                rows=_count(staging, "merged_keys"),
                row_conflicts=row_conflicts,
                cell_conflicts=cell_conflicts,
                key_conflicts=key_conflicts,
                decisions_applied=(
                    _count(staging, "decision_row")
                    + _count(staging, "decision_cell")
                    + _count(staging, "decision_key")
                ),
            )
        finally:
            staging.close()
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
