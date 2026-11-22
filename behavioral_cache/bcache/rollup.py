"""Incrementally advanced day/region sales rollup ledger.

``advance-sales-rollup`` maintains a small SQLite *ledger* next to the
snapshot registry.  Every advance pins one already-published immutable
snapshot (named explicitly, never ``current``), compares its ``sales`` and
``stores`` tables against the ledger's own copy of the previously received
contract columns, and folds only the *contribution changes* into the public
``rollup`` table.  Downstream readers query ``rollup`` / ``rollup_meta``
with plain SQL; one SQLite read transaction always sees a single consistent
ledger generation because each advance commits atomically.  No new snapshot
is produced and the job cache is not involved.

Contract consumed from the snapshot
-----------------------------------
* ``sales``: one row per sale keyed by ``row_id`` with non-NULL
  ``day:int64``, ``store:text``, ``buyer:text``, ``amount:int64``.
* ``stores``: lookup-keyed by ``store:text`` (declared ``key_col``) with
  non-NULL ``region:text``.

Missing tables, wrong column types, a missing lookup-key declaration or any
row violating the contract reject the whole advance.  Empty tables are
legal.  Other tables and columns never affect the ledger; strings match
byte-for-byte (no case folding, no trimming).

Ledger semantics
----------------
``rollup(day, region, gross, distinct_buyers)`` holds exactly one row per
(day, region) combination that still has sales.  A sale whose store is not
registered maps to SQL ``NULL`` region, distinct from every real region.
``gross`` is the signed sum of amounts (zero or negative allowed; a zero
net does not remove the row — losing the last sale does).
``distinct_buyers`` counts each buyer once per combination no matter how
many stores they bought from.  Final gross values must fit int64 or the
whole advance is rejected; intermediate sums are accumulated as Python
integers so a legal final result is never falsely rejected.

Incrementality
--------------
The first advance builds from empty state.  Later advances stream both
input tables of the target snapshot (no change list is required from the
snapshot and the previous snapshot may already be gone from the registry),
diff them inside SQLite against the retained state, and then touch only the
(day, region) combinations with contribution changes — never re-aggregating
unaffected days, never rebuilding the whole table.  Stores locate their
sales through an index on ``sales_state(store)``.  All intermediate rows
live in a temporary on-disk staging database; peak memory stays independent
of input and change sizes.

Revision protocol
-----------------
``rollup_meta`` holds the single row ``(revision, snapshot_id,
snapshot_digest)``; revisions start at 1.  An empty state is initialised
with ``--expect 0``; later advances must pass the current revision and
commit exactly one revision forward even when the content does not change.
The only retry tolerated: if the ledger already sits at ``expect + 1`` with
the same snapshot id and digest, the advance is reported as already
committed without writing again.  Anything else stale is an error.  Two
operators racing from the same revision serialise on the database write
lock: the loser's re-check fails and rolls back, so only one advance lands.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass, field
from typing import Any, Optional

from .encoding import MISSING
from .snapshot import Snapshot, SnapshotRegistry

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

SALES_CONTRACT = (
    ("day", "int64"),
    ("store", "text"),
    ("buyer", "text"),
    ("amount", "int64"),
)
STORES_CONTRACT = (
    ("store", "text"),
    ("region", "text"),
)

_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS rollup_meta (
    revision INTEGER NOT NULL,
    snapshot_id TEXT NOT NULL,
    snapshot_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rollup (
    day INTEGER NOT NULL,
    region TEXT,
    gross INTEGER NOT NULL,
    distinct_buyers INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS rollup_by_day_region ON rollup(day, region);
CREATE TABLE IF NOT EXISTS sales_state (
    row_id INTEGER PRIMARY KEY,
    day INTEGER NOT NULL,
    store TEXT NOT NULL,
    buyer TEXT NOT NULL,
    amount INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS sales_state_by_store ON sales_state(store);
CREATE TABLE IF NOT EXISTS stores_state (
    store TEXT PRIMARY KEY,
    region TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS buyer_presence (
    day INTEGER NOT NULL,
    region_enc TEXT NOT NULL,
    buyer TEXT NOT NULL,
    cnt INTEGER NOT NULL,
    PRIMARY KEY (day, region_enc, buyer)
);
"""

# Staging tables live in a scratch database ATTACHed as "stage"; they hold
# the streamed inputs, the diffs and the contribution events, so nothing
# intermediate is ever written into the ledger database outside the commit.
_STAGE_STATEMENTS = [
    """
    CREATE TABLE stage.new_sales(
        row_id INTEGER PRIMARY KEY,
        day INTEGER NOT NULL,
        store TEXT NOT NULL,
        buyer TEXT NOT NULL,
        amount INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE stage.new_stores(
        store TEXT PRIMARY KEY,
        region TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE stage.sale_delta(
        row_id INTEGER PRIMARY KEY,
        old_day INTEGER, old_store TEXT, old_buyer TEXT, old_amount INTEGER,
        new_day INTEGER, new_store TEXT, new_buyer TEXT, new_amount INTEGER
    )
    """,
    """
    CREATE TABLE stage.store_delta(
        store TEXT PRIMARY KEY,
        old_region TEXT,
        new_region TEXT
    )
    """,
    """
    CREATE TABLE stage.contrib(
        day INTEGER NOT NULL,
        region TEXT,
        buyer TEXT NOT NULL,
        amount INTEGER NOT NULL,
        delta INTEGER NOT NULL
    )
    """,
]

# Diff the freshly streamed inputs against the retained state.  Only the
# four contract columns of a sale and the store->region mapping itself are
# compared: unused columns and the physical row ids of store rows can never
# produce a delta.
_DIFF_STATEMENTS = [
    """
    INSERT INTO stage.sale_delta
    SELECT n.row_id,
           o.day, o.store, o.buyer, o.amount,
           n.day, n.store, n.buyer, n.amount
    FROM stage.new_sales n
    LEFT JOIN main.sales_state o ON o.row_id = n.row_id
    WHERE o.row_id IS NULL
       OR o.day IS NOT n.day
       OR o.store IS NOT n.store
       OR o.buyer IS NOT n.buyer
       OR o.amount IS NOT n.amount
    """,
    """
    INSERT INTO stage.sale_delta
    SELECT o.row_id,
           o.day, o.store, o.buyer, o.amount,
           NULL, NULL, NULL, NULL
    FROM main.sales_state o
    LEFT JOIN stage.new_sales n ON n.row_id = o.row_id
    WHERE n.row_id IS NULL
    """,
    """
    INSERT INTO stage.store_delta
    SELECT n.store, o.region, n.region
    FROM stage.new_stores n
    LEFT JOIN main.stores_state o ON o.store = n.store
    WHERE o.store IS NULL OR o.region IS NOT n.region
    """,
    """
    INSERT INTO stage.store_delta
    SELECT o.store, o.region, NULL
    FROM main.stores_state o
    LEFT JOIN stage.new_stores n ON n.store = o.store
    WHERE n.store IS NULL
    """,
]

# Contribution events.  ``amount`` stays the raw (never negated) int64 and
# ``delta`` is the +1/-1 sign: negating -2**63 in SQL would silently become
# a float, and summing amounts in SQL could overflow mid-way, so all
# arithmetic on events happens later in Python integers.
#
# Region resolution needs presence, not just value: a NULL region in
# store_delta is a real outcome (store unmapped), so COALESCE would
# wrongly fall through to the retained old mapping.  The LEFT JOIN + CASE
# below reads "if the store changed, use the delta's side, else the
# retained (unchanged) mapping; absent everywhere means SQL NULL".
_CONTRIB_STATEMENTS = [
    # Changed sales: withdraw the old contribution under the OLD region map.
    """
    INSERT INTO stage.contrib(day, region, buyer, amount, delta)
    SELECT s.old_day,
           CASE WHEN d.store IS NOT NULL THEN d.old_region
                ELSE m.region END,
           s.old_buyer, s.old_amount, -1
    FROM stage.sale_delta s
    LEFT JOIN stage.store_delta d ON d.store = s.old_store
    LEFT JOIN main.stores_state m ON m.store = s.old_store
    WHERE s.old_day IS NOT NULL
    """,
    # Changed sales: add the new contribution under the NEW region map.
    """
    INSERT INTO stage.contrib(day, region, buyer, amount, delta)
    SELECT s.new_day,
           CASE WHEN d.store IS NOT NULL THEN d.new_region
                ELSE m.region END,
           s.new_buyer, s.new_amount, 1
    FROM stage.sale_delta s
    LEFT JOIN stage.store_delta d ON d.store = s.new_store
    LEFT JOIN main.stores_state m ON m.store = s.new_store
    WHERE s.new_day IS NOT NULL
    """,
    # Unchanged sales whose store moved region (or appeared/disappeared):
    # withdraw from the old region.  Changed sales are already fully handled
    # above, so they must not be migrated a second time along with the store.
    # CROSS JOIN pins the changed-stores table as the outer loop so each
    # affected store locates its sales through sales_state_by_store instead
    # of rescanning the whole sales detail.
    """
    INSERT INTO stage.contrib(day, region, buyer, amount, delta)
    SELECT s.day, d.old_region, s.buyer, s.amount, -1
    FROM stage.store_delta d
    CROSS JOIN main.sales_state s ON s.store = d.store
    WHERE s.row_id NOT IN (SELECT row_id FROM stage.sale_delta)
    """,
    # ... and add to the new region.
    """
    INSERT INTO stage.contrib(day, region, buyer, amount, delta)
    SELECT s.day, d.new_region, s.buyer, s.amount, 1
    FROM stage.store_delta d
    CROSS JOIN main.sales_state s ON s.store = d.store
    WHERE s.row_id NOT IN (SELECT row_id FROM stage.sale_delta)
    """,
]

_APPLY_STATEMENTS = [
    """
    DELETE FROM main.sales_state
    WHERE row_id IN (SELECT row_id FROM stage.sale_delta)
    """,
    """
    INSERT INTO main.sales_state(row_id, day, store, buyer, amount)
    SELECT row_id, new_day, new_store, new_buyer, new_amount
    FROM stage.sale_delta
    WHERE new_day IS NOT NULL
    """,
    """
    DELETE FROM main.stores_state
    WHERE store IN (SELECT store FROM stage.store_delta)
    """,
    """
    INSERT INTO main.stores_state(store, region)
    SELECT store, new_region
    FROM stage.store_delta
    WHERE new_region IS NOT NULL
    """,
]


class RollupError(ValueError):
    """Base class for advance-sales-rollup failures."""


class RollupContractError(RollupError):
    """The target snapshot does not satisfy the sales/stores contract."""


class RollupRevisionError(RollupError):
    """``--expect`` does not match the ledger's current revision."""


class RollupOverflowError(RollupError):
    """A final day/region gross does not fit into a signed int64."""


@dataclass(frozen=True)
class RollupResult:
    revision: int
    snapshot_id: str
    snapshot_digest: str
    advanced: bool  # False when this exact advance was already committed
    stats: dict[str, int] = field(default_factory=dict)


def _enc_region(region: Optional[str]) -> str:
    """Lossless internal key for a region; SQL NULL stays distinguishable."""
    return "N:" if region is None else "R:" + region


def _read_meta(conn: sqlite3.Connection) -> Optional[tuple]:
    rows = conn.execute(
        "SELECT revision, snapshot_id, snapshot_digest FROM rollup_meta"
    ).fetchall()
    if len(rows) > 1:
        raise RollupError(
            "corrupt ledger: rollup_meta holds %d rows" % len(rows)
        )
    return tuple(rows[0]) if rows else None


def _classify(
    meta: Optional[tuple], expect: int, snapshot_id: str, digest: str
) -> str:
    """Decide whether this request may proceed, is a committed retry, or is
    stale.  Returns ``"proceed"`` or ``"retry"``; raises otherwise."""
    if meta is None:
        if expect == 0:
            return "proceed"
        raise RollupRevisionError(
            "ledger is empty (revision 0) but --expect %d was given" % expect
        )
    current, current_id, current_digest = meta
    if expect == current:
        return "proceed"
    if (
        current == expect + 1
        and current_id == snapshot_id
        and current_digest == digest
    ):
        return "retry"
    raise RollupRevisionError(
        "ledger is at revision %d (snapshot %s); cannot apply --expect %d"
        % (current, current_id, expect)
    )


def _check_table_contract(snapshot: Snapshot, conn: sqlite3.Connection) -> None:
    for table in ("sales", "stores"):
        if not snapshot.has_table(conn, table):
            raise RollupContractError(
                "snapshot %s has no %s table" % (snapshot.snapshot_id, table)
            )
    sales_schema = dict(snapshot.schema(conn, "sales"))
    for col, col_type in SALES_CONTRACT:
        actual = sales_schema.get(col)
        if actual is None:
            raise RollupContractError("sales is missing column %r" % col)
        if actual != col_type:
            raise RollupContractError(
                "sales.%s must be %s, got %s" % (col, col_type, actual)
            )
    stores_schema = dict(snapshot.schema(conn, "stores"))
    for col, col_type in STORES_CONTRACT:
        actual = stores_schema.get(col)
        if actual is None:
            raise RollupContractError("stores is missing column %r" % col)
        if actual != col_type:
            raise RollupContractError(
                "stores.%s must be %s, got %s" % (col, col_type, actual)
            )
    key_col = snapshot.key_col(conn, "stores")
    if key_col != "store":
        raise RollupContractError(
            "stores must declare 'store' as its lookup key, got %r" % (key_col,)
        )


def _cell(
    values: dict, col: str, col_type: str, table: str, row_id: Any
) -> Any:
    where = "%s row_id=%r column %r" % (table, row_id, col)
    value = values.get(col, MISSING)
    if value is MISSING:
        raise RollupContractError("%s is absent" % where)
    if value is None:
        raise RollupContractError("%s is NULL" % where)
    if col_type == "int64":
        if isinstance(value, bool) or not isinstance(value, int):
            raise RollupContractError(
                "%s must be int64, got %s" % (where, type(value).__name__)
            )
    elif not isinstance(value, str):
        raise RollupContractError(
            "%s must be text, got %s" % (where, type(value).__name__)
        )
    return value


def _stream_inputs(
    snapshot: Snapshot, conn: sqlite3.Connection
) -> tuple[int, int]:
    """Validate the contract and stream both tables into the staging db."""
    snap_conn = snapshot.connect()
    try:
        _check_table_contract(snapshot, snap_conn)
        sales_rows = 0
        batch: list[tuple] = []
        try:
            for row_id, values in snapshot.scan(snap_conn, "sales"):
                batch.append((
                    row_id,
                    _cell(values, "day", "int64", "sales", row_id),
                    _cell(values, "store", "text", "sales", row_id),
                    _cell(values, "buyer", "text", "sales", row_id),
                    _cell(values, "amount", "int64", "sales", row_id),
                ))
                if len(batch) >= 500:
                    conn.executemany(
                        "INSERT INTO stage.new_sales VALUES (?, ?, ?, ?, ?)",
                        batch,
                    )
                    sales_rows += len(batch)
                    batch.clear()
            if batch:
                conn.executemany(
                    "INSERT INTO stage.new_sales VALUES (?, ?, ?, ?, ?)", batch
                )
                sales_rows += len(batch)
            stores_rows = 0
            batch = []
            for row_id, values in snapshot.scan(snap_conn, "stores"):
                batch.append((
                    _cell(values, "store", "text", "stores", row_id),
                    _cell(values, "region", "text", "stores", row_id),
                ))
                if len(batch) >= 500:
                    conn.executemany(
                        "INSERT INTO stage.new_stores VALUES (?, ?)", batch
                    )
                    stores_rows += len(batch)
                    batch.clear()
            if batch:
                conn.executemany(
                    "INSERT INTO stage.new_stores VALUES (?, ?)", batch
                )
                stores_rows += len(batch)
        except sqlite3.IntegrityError as exc:
            raise RollupContractError(
                "snapshot %s violates the sales/stores contract: %s"
                % (snapshot.snapshot_id, exc)
            ) from exc
        return sales_rows, stores_rows
    finally:
        snap_conn.close()


class _ContribApplier:
    """Fold staged contribution events into ``buyer_presence`` and ``rollup``.

    Events are consumed ordered by (day, region, buyer) so only one buyer /
    one group is accumulated at a time.  All sums are Python integers: an
    intermediate overflow can never falsely reject a legal final gross.
    """

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.groups_touched = 0
        self.group: Optional[tuple] = None
        self.buyer: Optional[str] = None
        self.buyer_amount = 0
        self.buyer_count = 0
        self.gross_delta = 0
        self.buyers_delta = 0

    def add(self, day: int, region: Optional[str], buyer: str,
            amount: int, delta: int) -> None:
        group = (day, region)
        if group != self.group:
            self._flush_buyer()
            self._flush_group()
            self.group = group
        if buyer != self.buyer:
            self._flush_buyer()
            self.buyer = buyer
        self.buyer_amount += amount * delta
        self.buyer_count += delta

    def finish(self) -> int:
        self._flush_buyer()
        self._flush_group()
        return self.groups_touched

    def _flush_buyer(self) -> None:
        if self.buyer is None:
            return
        assert self.group is not None
        day, region = self.group
        self.gross_delta += self.buyer_amount
        if self.buyer_count:
            enc = _enc_region(region)
            row = self.conn.execute(
                "SELECT cnt FROM buyer_presence "
                "WHERE day = ? AND region_enc = ? AND buyer = ?",
                (day, enc, self.buyer),
            ).fetchone()
            old_cnt = row[0] if row is not None else 0
            new_cnt = old_cnt + self.buyer_count
            if new_cnt < 0:
                raise RollupError(
                    "ledger state inconsistent: buyer count underflow"
                )
            if old_cnt == 0 and new_cnt > 0:
                self.conn.execute(
                    "INSERT INTO buyer_presence(day, region_enc, buyer, cnt) "
                    "VALUES (?, ?, ?, ?)",
                    (day, enc, self.buyer, new_cnt),
                )
                self.buyers_delta += 1
            elif old_cnt > 0 and new_cnt == 0:
                self.conn.execute(
                    "DELETE FROM buyer_presence "
                    "WHERE day = ? AND region_enc = ? AND buyer = ?",
                    (day, enc, self.buyer),
                )
                self.buyers_delta -= 1
            elif new_cnt != old_cnt:
                self.conn.execute(
                    "UPDATE buyer_presence SET cnt = ? "
                    "WHERE day = ? AND region_enc = ? AND buyer = ?",
                    (new_cnt, day, enc, self.buyer),
                )
        self.buyer = None
        self.buyer_amount = 0
        self.buyer_count = 0

    def _flush_group(self) -> None:
        if self.group is None:
            return
        day, region = self.group
        row = self.conn.execute(
            "SELECT gross, distinct_buyers FROM rollup "
            "WHERE day = ? AND region IS ?",
            (day, region),
        ).fetchone()
        old_gross = row[0] if row is not None else 0
        old_buyers = row[1] if row is not None else 0
        new_gross = old_gross + self.gross_delta
        if not (INT64_MIN <= new_gross <= INT64_MAX):
            raise RollupOverflowError(
                "final gross for day=%r region=%r does not fit int64: %d"
                % (day, region, new_gross)
            )
        new_buyers = old_buyers + self.buyers_delta
        if new_buyers < 0:
            raise RollupError(
                "ledger state inconsistent: distinct buyer underflow"
            )
        if new_buyers == 0:
            # The last sale left this combination: remove the row.  A zero
            # gross with remaining sales would stay, by contrast.
            if row is not None:
                self.conn.execute(
                    "DELETE FROM rollup WHERE day = ? AND region IS ?",
                    (day, region),
                )
        elif row is None:
            self.conn.execute(
                "INSERT INTO rollup(day, region, gross, distinct_buyers) "
                "VALUES (?, ?, ?, ?)",
                (day, region, new_gross, new_buyers),
            )
        else:
            self.conn.execute(
                "UPDATE rollup SET gross = ?, distinct_buyers = ? "
                "WHERE day = ? AND region IS ?",
                (new_gross, new_buyers, day, region),
            )
        self.groups_touched += 1
        self.group = None
        self.gross_delta = 0
        self.buyers_delta = 0


def advance_sales_rollup(
    registry: SnapshotRegistry,
    snapshot_id: str,
    state_path: str,
    expect: int,
    *,
    scratch_root: Optional[str] = None,
) -> RollupResult:
    """Advance the ledger at ``state_path`` to ``snapshot_id``.

    The snapshot is resolved by its explicit id (``current`` is never
    consulted) and its content digest comes from the registry manifest.
    """
    if isinstance(expect, bool) or not isinstance(expect, int) or expect < 0:
        raise RollupError("--expect must be a non-negative integer")
    try:
        snapshot = registry.open(snapshot_id)
    except FileNotFoundError as exc:
        raise RollupError("unknown snapshot %s: %s" % (snapshot_id, exc)) from exc

    parent = os.path.dirname(os.path.abspath(state_path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(state_path, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.executescript(_STATE_SCHEMA)
        # Cheap pre-check so obviously stale requests fail before any I/O;
        # the authoritative check runs again inside the write transaction.
        meta = _read_meta(conn)
        if (
            _classify(meta, expect, snapshot.snapshot_id, snapshot.digest)
            == "retry"
        ):
            assert meta is not None
            return RollupResult(meta[0], meta[1], meta[2], advanced=False)

        scratch = tempfile.mkdtemp(prefix="rollup-", dir=scratch_root)
        try:
            conn.execute(
                "ATTACH DATABASE ? AS stage",
                (os.path.join(scratch, "stage.sqlite3"),),
            )
            try:
                for statement in _STAGE_STATEMENTS:
                    conn.execute(statement)
                sales_rows, stores_rows = _stream_inputs(snapshot, conn)

                try:
                    conn.execute("BEGIN IMMEDIATE")
                except sqlite3.OperationalError as exc:
                    if "locked" in str(exc):
                        raise RollupRevisionError(
                            "ledger is being advanced by another operator; "
                            "retry with the current revision"
                        ) from exc
                    raise
                try:
                    meta = _read_meta(conn)
                    if (
                        _classify(
                            meta, expect, snapshot.snapshot_id, snapshot.digest
                        )
                        == "retry"
                    ):
                        conn.rollback()
                        assert meta is not None
                        return RollupResult(
                            meta[0], meta[1], meta[2], advanced=False
                        )
                    for statement in _DIFF_STATEMENTS:
                        conn.execute(statement)
                    sale_changes = conn.execute(
                        "SELECT COUNT(*) FROM stage.sale_delta"
                    ).fetchone()[0]
                    store_changes = conn.execute(
                        "SELECT COUNT(*) FROM stage.store_delta"
                    ).fetchone()[0]
                    for statement in _CONTRIB_STATEMENTS:
                        conn.execute(statement)
                    contrib_events = conn.execute(
                        "SELECT COUNT(*) FROM stage.contrib"
                    ).fetchone()[0]

                    applier = _ContribApplier(conn)
                    cursor = conn.execute(
                        "SELECT day, region, buyer, amount, delta "
                        "FROM stage.contrib ORDER BY day, region, buyer"
                    )
                    for day, region, buyer, amount, delta in cursor:
                        applier.add(day, region, buyer, amount, delta)
                    groups_touched = applier.finish()

                    for statement in _APPLY_STATEMENTS:
                        conn.execute(statement)
                    new_revision = (meta[0] if meta is not None else 0) + 1
                    if meta is None:
                        conn.execute(
                            "INSERT INTO rollup_meta(revision, snapshot_id, "
                            "snapshot_digest) VALUES (?, ?, ?)",
                            (new_revision, snapshot.snapshot_id, snapshot.digest),
                        )
                    else:
                        updated = conn.execute(
                            "UPDATE rollup_meta SET revision = ?, "
                            "snapshot_id = ?, snapshot_digest = ? "
                            "WHERE revision = ?",
                            (
                                new_revision,
                                snapshot.snapshot_id,
                                snapshot.digest,
                                meta[0],
                            ),
                        )
                        if updated.rowcount != 1:
                            raise RollupRevisionError(
                                "ledger moved concurrently during commit"
                            )
                except BaseException:
                    conn.rollback()
                    raise
                conn.commit()
                return RollupResult(
                    revision=new_revision,
                    snapshot_id=snapshot.snapshot_id,
                    snapshot_digest=snapshot.digest,
                    advanced=True,
                    stats={
                        "sales_rows": sales_rows,
                        "stores_rows": stores_rows,
                        "sale_changes": int(sale_changes),
                        "store_changes": int(store_changes),
                        "contrib_events": int(contrib_events),
                        "groups_touched": groups_touched,
                    },
                )
            finally:
                try:
                    conn.execute("DETACH DATABASE stage")
                except sqlite3.Error:
                    pass
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
    finally:
        conn.close()
