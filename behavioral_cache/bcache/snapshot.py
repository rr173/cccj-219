"""Immutable, content-addressed data snapshots stored in SQLite.

A snapshot is a directory containing a single SQLite database plus a small
manifest.  Each relation stores typed rows keyed by ``row_id`` in an
entity-attribute-value layout, so column sets can differ across tables while
NULL cells and truly missing cells stay distinguishable.

Snapshots are *immutable* once finalised.  Derived snapshots are new
directories; a runner pins one snapshot for the whole validation/execution so
a concurrent publish can never mix generations.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional

from .encoding import (
    MISSING,
    StreamHasher,
    _uleb128,  # type: ignore[attr-defined]
    encode_schema,
    encode_value,
)
from .errors import SchemaError, SnapshotConflict, TableNotFound, UnknownColumn

DB_FILENAME = "snapshot.sqlite3"
MANIFEST_FILENAME = "manifest.txt"
POINTER_FILENAME = "current"
VALID_TYPES = {"int64", "text", "bool"}
FORMAT_VERSION = "1"


# ---------------------------------------------------------------------------
# Read-only handle
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Snapshot:
    path: str
    digest: str
    snapshot_id: str

    @property
    def db_path(self) -> str:
        return os.path.join(self.path, DB_FILENAME)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            "file:%s?mode=ro" % self.db_path, uri=True, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        return conn

    def table_names(self, conn: sqlite3.Connection) -> list[str]:
        rows = conn.execute("SELECT name FROM tables ORDER BY name").fetchall()
        return [r[0] for r in rows]

    def has_table(self, conn: sqlite3.Connection, table: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM tables WHERE name = ?", (table,)
        ).fetchone()
        return row is not None

    def schema(self, conn: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
        if not self.has_table(conn, table):
            raise TableNotFound(table)
        rows = conn.execute(
            "SELECT col_name, col_type FROM columns "
            "WHERE table_name = ? ORDER BY ordinal",
            (table,),
        ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def key_col(self, conn: sqlite3.Connection, table: str) -> Optional[str]:
        if not self.has_table(conn, table):
            raise TableNotFound(table)
        row = conn.execute(
            "SELECT key_col FROM tables WHERE name = ?", (table,)
        ).fetchone()
        return row[0]

    def scan(
        self, conn: sqlite3.Connection, table: str
    ):
        """Yield ``(row_id, row_dict)`` pairs in stable row_id order."""
        schema = self.schema(conn, table)
        col_names = [c for c, _ in schema]
        placeholders = ",".join("?" for _ in col_names)
        sql = (
            "SELECT row_id, col_name, value_type, int_value, text_value, bool_value "
            "FROM cells WHERE table_name = ? AND col_name IN (%s) "
            "ORDER BY row_id, col_name" % placeholders
        )
        current_id: Any = None
        current: dict[str, Any] = {}
        for row in conn.execute(sql, (table, *col_names)):
            rid = row[0]
            if rid != current_id:
                if current_id is not None:
                    yield current_id, current
                current_id = rid
                current = {c: MISSING for c in col_names}
            current[row[1]] = _decode_cell(row[2], row[3], row[4], row[5])
        if current_id is not None:
            yield current_id, current

    def list_partitions(
        self, conn: sqlite3.Connection, prefix: str
    ) -> list[str]:
        rows = conn.execute(
            "SELECT name FROM tables WHERE name LIKE ? ESCAPE '\\' ORDER BY name",
            (_like_prefix(prefix),),
        ).fetchall()
        return [r[0] for r in rows]

    def count_rows(self, conn: sqlite3.Connection, table: str) -> int:
        row = conn.execute(
            "SELECT COUNT(DISTINCT row_id) FROM cells WHERE table_name = ?",
            (table,),
        ).fetchone()
        return int(row[0])

    def fingerprint(self) -> str:
        """Digest of the exact on-disk bytes (logical digest lives in manifest).

        Used for cheap re-validation of the publish race: a snapshot moved into
        place is only trusted after its bytes are shown to match the staged copy.
        """
        import hashlib

        h = hashlib.blake2b(digest_size=32)
        with open(self.db_path, "rb") as fh:
            while True:
                chunk = fh.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()


def _like_prefix(prefix: str) -> str:
    out = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return out + "%"


def _decode_cell(
    value_type: str,
    int_value: Any,
    text_value: Any,
    bool_value: Any,
) -> Any:
    if value_type == "int64":
        return int(int_value)
    if value_type == "text":
        return text_value
    if value_type == "bool":
        return bool(bool_value)
    if value_type == "null":
        return None
    raise SchemaError("unknown stored value type %r" % (value_type,))


# ---------------------------------------------------------------------------
# Writable builder
# ---------------------------------------------------------------------------

class SnapshotBuilder:
    """Builds a new snapshot directory, optionally derived from an old one."""

    def __init__(self, path: str, derive_from: Optional[Snapshot] = None):
        self.path = path
        os.makedirs(path, exist_ok=True)
        if derive_from is not None:
            shutil.copyfile(derive_from.db_path, os.path.join(path, DB_FILENAME))
        self._conn = sqlite3.connect(os.path.join(path, DB_FILENAME))
        if derive_from is None:
            self._create_schema()
        self._table_cache: dict[str, list[tuple[str, str]]] = {}
        self._finalized = False

    @property
    def connection(self) -> sqlite3.Connection:
        """The staging database connection (used by bounded-memory derivatives)."""
        return self._conn

    def _create_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE tables (
                name TEXT PRIMARY KEY,
                key_col TEXT
            );
            CREATE TABLE columns (
                table_name TEXT NOT NULL,
                col_name TEXT NOT NULL,
                col_type TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                PRIMARY KEY (table_name, col_name)
            );
            CREATE TABLE cells (
                table_name TEXT NOT NULL,
                row_id INTEGER NOT NULL,
                col_name TEXT NOT NULL,
                value_type TEXT NOT NULL,
                int_value INTEGER,
                text_value TEXT,
                bool_value INTEGER
            );
            CREATE INDEX cells_lookup
                ON cells (table_name, row_id, col_name);
            CREATE INDEX cells_by_column
                ON cells (table_name, col_name, value_type, int_value,
                          text_value, bool_value);
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """
        )
        self._conn.commit()

    # -- schema -------------------------------------------------------------

    def add_table(
        self,
        name: str,
        schema: list[tuple[str, str]],
        key_col: Optional[str] = None,
    ) -> None:
        if self._finalized:
            raise SchemaError("snapshot already finalized")
        if not name or not isinstance(name, str):
            raise SchemaError("invalid table name")
        if not schema:
            raise SchemaError("table %r requires at least one column" % name)
        seen: set[str] = set()
        for ordinal, (col, col_type) in enumerate(schema):
            if not isinstance(col, str) or not col:
                raise SchemaError("invalid column name")
            if col in seen:
                raise SchemaError("duplicate column %r" % col)
            if col_type not in VALID_TYPES:
                raise SchemaError(
                    "column %r has unsupported type %r" % (col, col_type)
                )
            seen.add(col)
        if key_col is not None and key_col not in seen:
            raise SchemaError("key_col %r not in schema" % key_col)
        try:
            self._conn.execute(
                "INSERT INTO tables(name, key_col) VALUES (?, ?)",
                (name, key_col),
            )
        except sqlite3.IntegrityError as exc:
            raise SchemaError("table %r already exists" % name) from exc
        self._conn.executemany(
            "INSERT INTO columns(table_name, col_name, col_type, ordinal) "
            "VALUES (?, ?, ?, ?)",
            [(name, col, col_type, i) for i, (col, col_type) in enumerate(schema)],
        )
        self._table_cache[name] = list(schema)
        self._conn.commit()

    def _require_table(self, table: str) -> list[tuple[str, str]]:
        if table in self._table_cache:
            return self._table_cache[table]
        rows = self._conn.execute(
            "SELECT col_name, col_type FROM columns "
            "WHERE table_name = ? ORDER BY ordinal",
            (table,),
        ).fetchall()
        if not rows:
            raise TableNotFound(table)
        schema = [(r[0], r[1]) for r in rows]
        self._table_cache[table] = schema
        return schema

    def _key_col(self, table: str) -> Optional[str]:
        row = self._conn.execute(
            "SELECT key_col FROM tables WHERE name = ?", (table,)
        ).fetchone()
        return row[0] if row is not None else None

    @staticmethod
    def _stored_type(col_type: str, value: Any) -> str:
        # The *declared* column type decides the stored domain: a bool value in
        # an int64 column is a type error, not a new domain.
        if value is None:
            return "null"
        if col_type == "bool":
            return "bool"
        if col_type == "text":
            return "text"
        return "int64"

    # -- data ---------------------------------------------------------------

    def add_row(self, table: str, row_id: int, values: dict[str, Any]) -> None:
        if self._finalized:
            raise SchemaError("snapshot already finalized")
        if not isinstance(row_id, int) or isinstance(row_id, bool):
            raise SchemaError("row_id must be an int64 integer")
        schema = self._require_table(table)
        type_map = dict(schema)
        if extra := set(values) - set(type_map):
            raise SchemaError(
                "row for %r carries unknown columns %s"
                % (table, sorted(extra))
            )
        existing = self._conn.execute(
            "SELECT 1 FROM cells WHERE table_name = ? AND row_id = ? LIMIT 1",
            (table, row_id),
        ).fetchone()
        if existing is not None:
            raise SchemaError(
                "duplicate row_id %r in table %r" % (row_id, table)
            )
        key_col = self._key_col(table)
        if key_col is not None and key_col in values:
            key_value = values[key_col]
            if key_value is not None:
                stored_type = self._stored_type(
                    dict(schema)[key_col], key_value
                )
                if stored_type == "int64":
                    clash_sql = (
                        "SELECT 1 FROM cells WHERE table_name = ? AND "
                        "col_name = ? AND value_type = 'int64' "
                        "AND int_value = ? LIMIT 1"
                    )
                    params = (table, key_col, key_value)
                elif stored_type == "text":
                    clash_sql = (
                        "SELECT 1 FROM cells WHERE table_name = ? AND "
                        "col_name = ? AND value_type = 'text' "
                        "AND text_value = ? LIMIT 1"
                    )
                    params = (table, key_col, key_value)
                else:
                    clash_sql = (
                        "SELECT 1 FROM cells WHERE table_name = ? AND "
                        "col_name = ? AND value_type = 'bool' "
                        "AND bool_value = ? LIMIT 1"
                    )
                    params = (table, key_col, 1 if key_value else 0)
                if self._conn.execute(clash_sql, params).fetchone() is not None:
                    raise SchemaError(
                        "duplicate key %r=%r in table %r"
                        % (key_col, key_value, table)
                    )
        payload = []
        for col, col_type in schema:
            if col not in values:
                continue  # missing cell: no row at all
            value = values[col]
            if value is None:
                payload.append((table, row_id, col, "null", None, None, None))
                continue
            if col_type == "int64":
                if isinstance(value, bool) or not isinstance(value, int):
                    raise SchemaError(
                        "column %r expects int64, got %r" % (col, type(value))
                    )
                if not (-(2**63) <= value <= 2**63 - 1):
                    raise SchemaError("int64 out of range for column %r" % col)
                payload.append((table, row_id, col, "int64", value, None, None))
            elif col_type == "text":
                if not isinstance(value, str):
                    raise SchemaError(
                        "column %r expects text, got %r" % (col, type(value))
                    )
                payload.append((table, row_id, col, "text", None, value, None))
            else:  # bool
                if not isinstance(value, bool):
                    raise SchemaError(
                        "column %r expects bool, got %r" % (col, type(value))
                    )
                payload.append(
                    (table, row_id, col, "bool", None, None, 1 if value else 0)
                )
        self._conn.executemany(
            "INSERT INTO cells(table_name, row_id, col_name, value_type, "
            "int_value, text_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?)",
            payload,
        )

    def delete_row(self, table: str, row_id: int) -> bool:
        if self._finalized:
            raise SchemaError("snapshot already finalized")
        self._require_table(table)
        cur = self._conn.execute(
            "DELETE FROM cells WHERE table_name = ? AND row_id = ?",
            (table, row_id),
        )
        return cur.rowcount > 0

    def update_cell(
        self, table: str, row_id: int, column: str, value: Any
    ) -> None:
        """Set one cell; ``value=None`` writes an explicit NULL."""
        if self._finalized:
            raise SchemaError("snapshot already finalized")
        schema = dict(self._require_table(table))
        if column not in schema:
            raise UnknownColumn(table, column)
        exists = self._conn.execute(
            "SELECT 1 FROM cells WHERE table_name = ? AND row_id = ? LIMIT 1",
            (table, row_id),
        ).fetchone()
        if exists is None:
            raise SchemaError(
                "no row_id %r in table %r" % (row_id, table)
            )
        self._conn.execute(
            "DELETE FROM cells WHERE table_name = ? AND row_id = ? AND col_name = ?",
            (table, row_id, column),
        )
        placeholder = {"table": table, "row_id": row_id, "col": column}
        if value is None:
            self._conn.execute(
                "INSERT INTO cells(table_name, row_id, col_name, value_type) "
                "VALUES (:table, :row_id, :col, 'null')",
                placeholder,
            )
            return
        col_type = schema[column]
        if col_type == "int64":
            if isinstance(value, bool) or not isinstance(value, int):
                raise SchemaError("column %r expects int64" % column)
            self._conn.execute(
                "INSERT INTO cells(table_name, row_id, col_name, value_type, "
                "int_value) VALUES (:table, :row_id, :col, 'int64', :v)",
                {**placeholder, "v": value},
            )
        elif col_type == "text":
            if not isinstance(value, str):
                raise SchemaError("column %r expects text" % column)
            self._conn.execute(
                "INSERT INTO cells(table_name, row_id, col_name, value_type, "
                "text_value) VALUES (:table, :row_id, :col, 'text', :v)",
                {**placeholder, "v": value},
            )
        else:
            if not isinstance(value, bool):
                raise SchemaError("column %r expects bool" % column)
            self._conn.execute(
                "INSERT INTO cells(table_name, row_id, col_name, value_type, "
                "bool_value) VALUES (:table, :row_id, :col, 'bool', :v)",
                {**placeholder, "v": 1 if value else 0},
            )

    # -- finalisation -------------------------------------------------------

    def compute_digest(self) -> str:
        """Content digest over tables, schemas and typed rows in canonical order.

        The scan is streamed table by table and row by row (one row's cells are
        buffered at most), so digesting a large snapshot never materialises its
        row id set or its rows.
        """
        hasher = StreamHasher(namespace="snapshot-v" + FORMAT_VERSION)
        names = [
            r[0]
            for r in self._conn.execute("SELECT name FROM tables ORDER BY name")
        ]
        hasher.update_encoded(_uleb128(len(names)))
        for name in names:
            key_col = self._conn.execute(
                "SELECT key_col FROM tables WHERE name = ?", (name,)
            ).fetchone()[0]
            schema_rows = self._conn.execute(
                "SELECT col_name, col_type FROM columns "
                "WHERE table_name = ? ORDER BY ordinal",
                (name,),
            ).fetchall()
            schema = [(r[0], r[1]) for r in schema_rows]
            # Table frame: name, key column (or missing), typed schema.
            hasher.update_encoded(encode_value(name))
            hasher.update_encoded(
                encode_value(key_col) if key_col is not None else encode_value(MISSING)
            )
            hasher.update_encoded(encode_schema(schema))
            row_count = self._conn.execute(
                "SELECT COUNT(DISTINCT row_id) FROM cells WHERE table_name = ?",
                (name,),
            ).fetchone()[0]
            hasher.update_encoded(_uleb128(int(row_count)))
            self._digest_rows(name, schema, hasher)
        return hasher.hexdigest()

    def _digest_rows(
        self,
        table: str,
        schema: list[tuple[str, str]],
        hasher: StreamHasher,
    ) -> None:
        cursor = self._conn.execute(
            "SELECT row_id, col_name, value_type, int_value, text_value, "
            "bool_value FROM cells WHERE table_name = ? ORDER BY row_id, col_name",
            (table,),
        )
        wanted = {col for col, _ in schema}
        current_id: Any = None
        cell_map: dict[str, Any] = {}
        for raw in cursor:
            rid = raw[0]
            if rid != current_id:
                if current_id is not None:
                    self._digest_one_row(current_id, schema, cell_map, hasher)
                current_id = rid
                cell_map = {}
            if raw[1] in wanted:
                cell_map[raw[1]] = _decode_cell(raw[2], raw[3], raw[4], raw[5])
        if current_id is not None:
            self._digest_one_row(current_id, schema, cell_map, hasher)

    @staticmethod
    def _digest_one_row(
        row_id: Any,
        schema: list[tuple[str, str]],
        cell_map: dict[str, Any],
        hasher: StreamHasher,
    ) -> None:
        hasher.update_encoded(encode_value(row_id))
        for col, _ in schema:
            hasher.update_encoded(encode_value(cell_map.get(col, MISSING)))

    def finalize(self, snapshot_id: Optional[str] = None) -> Snapshot:
        self._conn.commit()
        digest = self.compute_digest()
        if snapshot_id is None:
            snapshot_id = digest[:16]
        self._conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
            ("snapshot_digest", digest),
        )
        self._conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
            ("snapshot_id", snapshot_id),
        )
        self._conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
            ("format_version", FORMAT_VERSION),
        )
        self._conn.commit()
        self._conn.close()
        self._finalized = True
        with open(os.path.join(self.path, MANIFEST_FILENAME), "w", encoding="utf-8") as fh:
            fh.write("snapshot_id=%s\ndigest=%s\nformat_version=%s\n"
                     % (snapshot_id, digest, FORMAT_VERSION))
        return Snapshot(self.path, digest, snapshot_id)


# ---------------------------------------------------------------------------
# Registry of published snapshots
# ---------------------------------------------------------------------------

class SnapshotRegistry:
    """A directory of published immutable snapshots with a current pointer."""

    def __init__(self, root: str):
        self.root = root
        self.snapshots_dir = os.path.join(root, "snapshots")
        os.makedirs(self.snapshots_dir, exist_ok=True)
        self.pointer_path = os.path.join(root, POINTER_FILENAME)

    def publish(self, snapshot: Snapshot) -> None:
        """Register an already-finalised snapshot under its id and make current."""
        self.publish_without_current(snapshot)
        self.set_current(snapshot.snapshot_id)

    def publish_without_current(self, snapshot: Snapshot) -> str:
        """Register a snapshot under its id without touching ``current``.

        Returns ``"unchanged"`` when the id already holds identical content,
        ``"published"`` when the snapshot was newly moved into place.
        Identical content under the same id is an idempotent success; different
        content under an existing id raises :class:`SnapshotConflict` *before*
        anything is moved, so no partial or overwriting publish can happen.
        """
        target = os.path.join(self.snapshots_dir, snapshot.snapshot_id)
        if os.path.exists(target):
            existing = self.open(snapshot.snapshot_id)
            if existing.digest == snapshot.digest:
                return "unchanged"
            raise SnapshotConflict(
                "snapshot id %s already holds different content"
                % snapshot.snapshot_id
            )
        # The staged copy is already complete and validated by the builder;
        # moving the directory is the atomic publish point.
        tmp = target + ".tmp-%d-%s" % (os.getpid(), id(snapshot))
        shutil.copytree(snapshot.path, tmp)
        try:
            if not os.path.exists(target):
                os.rename(tmp, target)
                return "published"
            # Someone published concurrently; judge by content, not by timing.
            existing = self.open(snapshot.snapshot_id)
            if existing.digest != snapshot.digest:
                raise SnapshotConflict(
                    "snapshot id %s already holds different content"
                    % snapshot.snapshot_id
                )
            if existing.fingerprint() != snapshot.fingerprint():
                raise SnapshotConflict(
                    "snapshot id %s was published with different bytes"
                    % snapshot.snapshot_id
                )
            return "unchanged"
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def set_current(self, snapshot_id: str) -> None:
        tmp = self.pointer_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(snapshot_id)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.pointer_path)

    def current_id(self) -> str:
        if not os.path.exists(self.pointer_path):
            raise FileNotFoundError("no current snapshot published in %s" % self.root)
        with open(self.pointer_path, encoding="utf-8") as fh:
            return fh.read().strip()

    def current(self) -> Snapshot:
        return self.open(self.current_id())

    def open(self, snapshot_id: str) -> Snapshot:
        path = os.path.join(self.snapshots_dir, snapshot_id)
        manifest = os.path.join(path, MANIFEST_FILENAME)
        if not os.path.exists(manifest):
            raise FileNotFoundError("unknown snapshot %s" % snapshot_id)
        digest = None
        with open(manifest, encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("digest="):
                    digest = line.split("=", 1)[1].strip()
        if digest is None:
            raise RuntimeError("manifest missing digest in %s" % path)
        return Snapshot(path, digest, snapshot_id)

    def pin(self, snapshot_id: Optional[str] = None) -> Snapshot:
        """Resolve the snapshot once and hold it for the whole operation."""
        if snapshot_id is None:
            snapshot_id = self.current_id()
        return self.open(snapshot_id)
