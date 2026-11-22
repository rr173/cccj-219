"""Read-behaviour evidence: ordered call log plus streamed read rows.

Evidence lives in a small SQLite database so a million-row ``read`` never has
to be held in memory.  Each call record stores the fully-specified arguments;
``read`` calls additionally stream their returned rows into ``read_rows`` in
delivery order, including the row's typed projection (NULL and missing
columns kept distinct) and the schema of the returned relation.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Optional

from .encoding import MISSING, canonical_json, digest_schema


class EvidenceWriter:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.executescript(
            """
            CREATE TABLE calls (
                seq INTEGER PRIMARY KEY,
                method TEXT NOT NULL,
                args_json TEXT NOT NULL,
                outcome TEXT NOT NULL,
                error_type TEXT,
                result_digest TEXT NOT NULL,
                detail_json TEXT NOT NULL
            );
            CREATE TABLE read_rows (
                seq INTEGER NOT NULL,
                row_idx INTEGER NOT NULL,
                row_id INTEGER NOT NULL,
                projection_json TEXT NOT NULL,
                PRIMARY KEY (seq, row_idx)
            );
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """
        )
        self._conn.commit()
        self._next_seq = 0

    def close(self) -> None:
        self._conn.close()

    def begin_call(self) -> int:
        seq = self._next_seq
        self._next_seq += 1
        return seq

    def write_simple_call(
        self,
        seq: int,
        method: str,
        args: dict[str, Any],
        outcome: str,
        result_digest: str,
        detail: dict[str, Any],
        error_type: Optional[str] = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO calls(seq, method, args_json, outcome, error_type, "
            "result_digest, detail_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                seq,
                method,
                canonical_json(args).decode("utf-8"),
                outcome,
                error_type,
                result_digest,
                canonical_json(detail).decode("utf-8"),
            ),
        )
        self._conn.commit()

    def begin_read_call(
        self, seq: int, args: dict[str, Any], schema: list[tuple[str, str]]
    ) -> None:
        detail = {
            "schema": [{"name": n, "type": t} for n, t in schema],
            "schema_digest": digest_schema(schema),
            "truncated": False,
            "row_count": 0,
        }
        self.write_simple_call(
            seq,
            "read",
            args,
            outcome="ok",
            result_digest="",  # filled in finish_read_call
            detail=detail,
        )

    def write_read_row(
        self, seq: int, row_idx: int, row_id: int, projection: dict[str, Any]
    ) -> None:
        self._conn.execute(
            "INSERT INTO read_rows(seq, row_idx, row_id, projection_json) "
            "VALUES (?, ?, ?, ?)",
            (
                seq,
                row_idx,
                row_id,
                canonical_json(_json_safe(projection)).decode("utf-8"),
            ),
        )

    def finish_read_call(
        self, seq: int, result_digest: str, row_count: int, truncated: bool
    ) -> None:
        detail = dict(self._load_detail(seq))
        detail["row_count"] = row_count
        detail["truncated"] = truncated
        self._conn.execute(
            "UPDATE calls SET result_digest = ?, detail_json = ? WHERE seq = ?",
            (
                result_digest,
                canonical_json(detail).decode("utf-8"),
                seq,
            ),
        )
        self._conn.commit()

    def _load_detail(self, seq: int) -> dict[str, Any]:
        import json

        row = self._conn.execute(
            "SELECT detail_json FROM calls WHERE seq = ?", (seq,)
        ).fetchone()
        return json.loads(row[0])

    def set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", (key, value)
        )
        self._conn.commit()


def _json_safe(value: Any) -> Any:
    if value is MISSING:
        return {"__tag__": "missing"}
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def project_row(
    row: dict[str, Any], columns: list[str]
) -> dict[str, Any]:
    """Project requested columns; absent cells stay MISSING, NULL stays None."""
    return {c: row.get(c, MISSING) for c in columns}
