"""advance-sales-rollup: incremental day/region sales ledger over snapshots.

Covers the hand-computed specification example, equality with a from-scratch
reference under random mutations, irrelevance of unused columns and store
row ids, contract rejection without partial state, the revision protocol
(including the single committed-retry exception), int64 edge cases, change
locality with on-demand generated unrelated day/regions, and real
multi-process competition through the CLI.
"""

import json
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bcache import SnapshotBuilder, SnapshotRegistry  # noqa: E402
from bcache.rollup import (  # noqa: E402
    RollupContractError,
    RollupError,
    RollupOverflowError,
    RollupRevisionError,
    advance_sales_rollup,
)

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENTRY = os.path.join(PROJECT, "bcache_run.py")

SALES_COLS = [("day", "int64"), ("store", "text"), ("buyer", "text"),
              ("amount", "int64")]
STORES_COLS = [("store", "text"), ("region", "text")]

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


def sale(day, store, buyer, amount, **extra):
    return dict(day=day, store=store, buyer=buyer, amount=amount, **extra)


def store(region, **extra):
    return dict(region=region, **extra)


def dump_ledger(state_path):
    """Read both public tables inside one SQLite read transaction."""
    conn = sqlite3.connect("file:%s?mode=ro" % state_path, uri=True,
                           isolation_level=None)
    try:
        conn.execute("BEGIN")
        meta = conn.execute(
            "SELECT revision, snapshot_id, snapshot_digest FROM rollup_meta"
        ).fetchall()
        rows = conn.execute(
            "SELECT day, region, gross, distinct_buyers FROM rollup"
        ).fetchall()
        conn.commit()
        return meta, {(d, r): (g, b) for d, r, g, b in rows}
    finally:
        conn.close()


def reference_rollup(snapshot):
    """From-scratch aggregation of one snapshot: the business definition."""
    conn = snapshot.connect()
    try:
        stores = {}
        for _rid, values in snapshot.scan(conn, "stores"):
            stores[values["store"]] = values["region"]
        groups = {}
        for _rid, values in snapshot.scan(conn, "sales"):
            key = (values["day"], stores.get(values["store"]))
            entry = groups.setdefault(key, [0, set()])
            entry[0] += values["amount"]
            entry[1].add(values["buyer"])
        return {key: (gross, len(buyers))
                for key, (gross, buyers) in groups.items()}
    finally:
        conn.close()


class RollupTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.registry = SnapshotRegistry(os.path.join(self.tmp, "registry"))
        self.state = os.path.join(self.tmp, "ledger", "rollup.sqlite3")
        self._counter = 0

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def publish(self, sales=(), stores=(), *, name=None, sales_schema=None,
                stores_schema=None, stores_key="store", drop_sales=False,
                drop_stores=False, extra_tables=None):
        """Build and publish a snapshot without ever touching ``current``."""
        self._counter += 1
        snap_id = name or "snap-%d" % self._counter
        builder = SnapshotBuilder(os.path.join(self.tmp, "build-" + snap_id))
        if not drop_sales:
            builder.add_table("sales", sales_schema or SALES_COLS)
            for row_id, values in sales:
                builder.add_row("sales", row_id, values)
        if not drop_stores:
            builder.add_table("stores", stores_schema or STORES_COLS,
                              key_col=stores_key)
            for row_id, values in stores:
                builder.add_row("stores", row_id, values)
        for table_name, (schema, key_col, rows) in (extra_tables or {}).items():
            builder.add_table(table_name, schema, key_col=key_col)
            for row_id, values in rows:
                builder.add_row(table_name, row_id, values)
        snap = builder.finalize(snapshot_id=snap_id)
        self.registry.publish_without_current(snap)
        return snap

    def advance(self, snapshot, expect):
        return advance_sales_rollup(
            self.registry, snapshot.snapshot_id, self.state, expect
        )

    def assertLedger(self, expect_revision, snapshot, expected=None):
        meta, rows = dump_ledger(self.state)
        self.assertEqual(len(meta), 1, "rollup_meta must hold exactly one row")
        revision, snap_id, digest = meta[0]
        self.assertEqual(revision, expect_revision)
        self.assertEqual(snap_id, snapshot.snapshot_id)
        self.assertEqual(digest, snapshot.digest)
        want = reference_rollup(snapshot) if expected is None else expected
        self.assertEqual(rows, want)


class SpecExampleTests(RollupTestBase):
    """The hand-computed scenario from the specification."""

    def test_spec_example(self):
        snap1 = self.publish(
            sales=[
                (1, sale(100, "A", "u", 10)),
                (2, sale(100, "A", "v", 0)),
                (3, sale(100, "B", "u", 20)),
                (4, sale(100, "B", "w", -5)),
                (5, sale(100, "A", "u", 8)),
                (6, sale(100, "C", "u", 3)),
            ],
            stores=[
                (1, {"store": "A", "region": "N"}),
                (2, {"store": "B", "region": "S"}),
            ],
        )
        result = self.advance(snap1, 0)
        self.assertTrue(result.advanced)
        self.assertEqual(result.revision, 1)
        self.assertLedger(1, snap1, expected={
            (100, "N"): (18, 2),
            (100, "S"): (15, 2),
            (100, None): (3, 1),
        })

        # A moves to S, B is removed, C is registered into N; sale 1 becomes
        # (B, u, 30), sale 2 is deleted.  Sale 1 must NOT be migrated along
        # with store A a second time, and the two unmapped sales of buyer u
        # (rows 1 and 3) count as one buyer.
        snap2 = self.publish(
            sales=[
                (1, sale(100, "B", "u", 30)),
                (3, sale(100, "B", "u", 20)),
                (4, sale(100, "B", "w", -5)),
                (5, sale(100, "A", "u", 8)),
                (6, sale(100, "C", "u", 3)),
            ],
            stores=[
                (1, {"store": "A", "region": "S"}),
                (3, {"store": "C", "region": "N"}),
            ],
        )
        result = self.advance(snap2, 1)
        self.assertEqual(result.revision, 2)
        self.assertLedger(2, snap2, expected={
            (100, "N"): (3, 1),
            (100, "S"): (8, 1),
            (100, None): (45, 2),
        })

    def test_empty_tables_are_legal(self):
        snap = self.publish(sales=[], stores=[])
        result = self.advance(snap, 0)
        self.assertEqual(result.revision, 1)
        self.assertLedger(1, snap, expected={})
        # Sales arriving later advance the same ledger normally.
        snap2 = self.publish(
            sales=[(1, sale(7, "A", "u", 4))],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.advance(snap2, 1)
        self.assertLedger(2, snap2, expected={(7, "N"): (4, 1)})

    def test_zero_amount_sale_keeps_its_group(self):
        snap1 = self.publish(
            sales=[(1, sale(5, "A", "u", 0))],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.advance(snap1, 0)
        # A zero net is not the same as "no sales": the row must exist.
        self.assertLedger(1, snap1, expected={(5, "N"): (0, 1)})

        snap2 = self.publish(
            sales=[],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.advance(snap2, 1)
        # The last sale was deleted: the combination is removed.
        self.assertLedger(2, snap2, expected={})

    def test_last_sale_migrating_away_removes_old_group(self):
        snap1 = self.publish(
            sales=[(1, sale(5, "A", "u", 9))],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.advance(snap1, 0)
        snap2 = self.publish(
            sales=[(1, sale(5, "B", "u", 9))],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.advance(snap2, 1)
        self.assertLedger(2, snap2, expected={(5, None): (9, 1)})

    def test_buyer_counted_once_across_stores(self):
        snap = self.publish(
            sales=[
                (1, sale(3, "A", "u", 5)),
                (2, sale(3, "B", "u", 7)),   # B unmapped
                (3, sale(3, "C", "u", 9)),   # C unmapped
                (4, sale(3, "C", "w", 1)),
            ],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.advance(snap, 0)
        self.assertLedger(1, snap, expected={
            (3, "N"): (5, 1),
            (3, None): (17, 2),  # u at two unmapped stores counts once
        })


class EqualityAndLocalityTests(RollupTestBase):
    def test_unrelated_columns_and_store_rowids_leave_ledger_untouched(self):
        sales_schema = SALES_COLS + [("note", "text")]
        stores_schema = STORES_COLS + [("manager", "text")]
        snap1 = self.publish(
            sales=[(1, sale(1, "A", "u", 10, note="x")),
                   (2, sale(1, "B", "v", 20, note="y"))],
            stores=[(11, {"store": "A", "region": "N", "manager": "amy"}),
                    (22, {"store": "B", "region": "S", "manager": "bob"})],
            sales_schema=sales_schema,
            stores_schema=stores_schema,
        )
        self.advance(snap1, 0)
        _, before = dump_ledger(self.state)

        # Same contract content: only unused columns, store row ids and an
        # unrelated extra table change.
        snap2 = self.publish(
            sales=[(1, sale(1, "A", "u", 10, note="CHANGED")),
                   (2, sale(1, "B", "v", 20, note="also changed"))],
            stores=[(77, {"store": "A", "region": "N", "manager": "zoe"}),
                    (88, {"store": "B", "region": "S", "manager": "yan"})],
            sales_schema=sales_schema,
            stores_schema=stores_schema,
            extra_tables={"audit": ([("k", "int64")], None,
                                    [(1, {"k": 5})])},
        )
        result = self.advance(snap2, 1)
        self.assertEqual(result.revision, 2)
        self.assertEqual(result.stats["sale_changes"], 0)
        self.assertEqual(result.stats["store_changes"], 0)
        self.assertEqual(result.stats["contrib_events"], 0)
        self.assertEqual(result.stats["groups_touched"], 0)
        _, after = dump_ledger(self.state)
        self.assertEqual(before, after)
        self.assertLedger(2, snap2)

    def test_randomized_mutations_match_full_recompute(self):
        rng = random.Random(20261122)
        regions = ["N", "S", "E", "W"]
        buyers = ["u%d" % i for i in range(10)]
        days = [100, 101, 102]
        sales = {}    # row_id -> values dict (current truth)
        stores = {}   # store name -> region (current truth)
        store_rowids = {}
        next_row_id = [1]
        revision = 0

        def random_sale(rid):
            return sale(
                rng.choice(days),
                "st-%d" % rng.randrange(8),
                rng.choice(buyers),
                rng.randint(-500, 500),
                note="n%d" % rng.randrange(100),
            )

        for step in range(14):
            for _ in range(rng.randint(1, 7)):
                op = rng.choice(
                    ["upsert_sale"] * 4 + ["del_sale", "move_store",
                                           "del_store", "add_store",
                                           "permute_store_rowids",
                                           "touch_notes"])
                if op == "upsert_sale":
                    rid = rng.randrange(1, next_row_id[0] + 3)
                    sales[rid] = random_sale(rid)
                    next_row_id[0] = max(next_row_id[0], rid + 1)
                elif op == "del_sale" and sales:
                    sales.pop(rng.choice(sorted(sales)))
                elif op == "move_store" and stores:
                    name = rng.choice(sorted(stores))
                    stores[name] = rng.choice(regions)
                elif op == "del_store" and stores:
                    stores.pop(rng.choice(sorted(stores)))
                elif op == "add_store":
                    stores["st-%d" % rng.randrange(8)] = rng.choice(regions)
                elif op == "permute_store_rowids":
                    store_rowids = {
                        name: rng.randrange(1, 10**6) for name in stores
                    }
                else:  # touch_notes: change only the unused column
                    for rid in sales:
                        sales[rid]["note"] = "t%d" % rng.randrange(1000)
            used_rowids = set()
            store_rows = []
            for name in sorted(stores):
                rid = store_rowids.get(name)
                while rid is None or rid in used_rowids:
                    rid = rng.randrange(1, 10**6)
                used_rowids.add(rid)
                store_rowids[name] = rid
                store_rows.append(
                    (rid, {"store": name, "region": stores[name],
                           "manager": "m%d" % rng.randrange(50)}))
            snap = self.publish(
                sales=[(rid, sales[rid]) for rid in sorted(sales)],
                stores=store_rows,
                sales_schema=SALES_COLS + [("note", "text")],
                stores_schema=STORES_COLS + [("manager", "text")],
                name="rnd-%d" % step,
            )
            result = self.advance(snap, revision)
            revision += 1
            self.assertEqual(result.revision, revision)
            self.assertLedger(revision, snap)

    def test_locality_with_generated_unrelated_day_regions(self):
        # Hundreds of on-demand generated, unrelated (day, region) groups.
        group_count = 400
        sales_rows = []
        store_rows = []
        for i in range(group_count):
            store_rows.append(
                (i + 1, {"store": "st-%d" % i, "region": "RG%d" % (i % 7)}))
            sales_rows.append(
                (i + 1, sale(1000 + i, "st-%d" % i, "buyer-%d" % i, i)))
        snap1 = self.publish(sales=sales_rows, stores=store_rows)
        result = self.advance(snap1, 0)
        groups = len(reference_rollup(snap1))
        self.assertEqual(result.stats["groups_touched"], groups)
        self.assertGreater(groups, 300)

        # One sale's amount changes: exactly one group may be touched.
        sales2 = [dict(row) for _rid, row in sales_rows]
        sales2[6]["amount"] += 1000
        snap2 = self.publish(sales=list(zip([r for r, _ in sales_rows],
                                            sales2)),
                             stores=store_rows)
        result = self.advance(snap2, 1)
        self.assertEqual(result.stats["sale_changes"], 1)
        self.assertEqual(result.stats["contrib_events"], 2)
        self.assertEqual(result.stats["groups_touched"], 1)
        self.assertLedger(2, snap2)

        # One store moves region: only the groups its own sales touch.
        store_rows3 = [dict(row) for _rid, row in store_rows]
        store_rows3[8]["region"] = "ELSEWHERE"
        snap3 = self.publish(
            sales=list(zip([r for r, _ in sales_rows], sales2)),
            stores=list(zip([r for r, _ in store_rows], store_rows3)),
        )
        result = self.advance(snap3, 2)
        self.assertEqual(result.stats["sale_changes"], 0)
        self.assertEqual(result.stats["store_changes"], 1)
        self.assertLessEqual(result.stats["groups_touched"], 2)
        self.assertLessEqual(result.stats["contrib_events"], 2)
        self.assertLedger(3, snap3)

    def test_store_moves_locate_sales_through_the_index(self):
        # The store-move statements must drive from the changed-stores table
        # and look sales up via sales_state_by_store, never rescan the whole
        # sales detail per changed store.
        from bcache.rollup import (
            _CONTRIB_STATEMENTS,
            _STATE_SCHEMA,
            _STAGE_STATEMENTS,
        )

        conn = sqlite3.connect(":memory:", isolation_level=None)
        conn.executescript(_STATE_SCHEMA)
        conn.execute("ATTACH DATABASE ':memory:' AS stage")
        for statement in _STAGE_STATEMENTS:
            conn.execute(statement)
        plans = []
        for statement in _CONTRIB_STATEMENTS[2:]:
            plans = conn.execute(
                "EXPLAIN QUERY PLAN " + statement.strip().rstrip(";")
            ).fetchall()
            detail = " | ".join(row[3] for row in plans)
            self.assertIn("sales_state_by_store", detail)
            self.assertNotIn("SCAN s", detail)
        conn.close()

    def test_previous_snapshot_may_leave_the_registry(self):
        snap1 = self.publish(sales=[(1, sale(1, "A", "u", 1))],
                             stores=[(1, {"store": "A", "region": "N"})])
        snap2 = self.publish(sales=[(1, sale(1, "A", "u", 2))],
                             stores=[(1, {"store": "A", "region": "N"})])
        self.advance(snap1, 0)
        self.advance(snap2, 1)
        shutil.rmtree(os.path.join(self.registry.snapshots_dir,
                                   snap1.snapshot_id))
        snap3 = self.publish(sales=[(1, sale(1, "A", "u", 3))],
                             stores=[(1, {"store": "A", "region": "S"})])
        result = self.advance(snap3, 2)
        self.assertEqual(result.revision, 3)
        self.assertLedger(3, snap3)


class ContractTests(RollupTestBase):
    def good_snapshot(self):
        return self.publish(
            sales=[(1, sale(1, "A", "u", 10))],
            stores=[(1, {"store": "A", "region": "N"})],
        )

    def assert_rejected_cleanly(self, snap, expect=0):
        with self.assertRaises(RollupContractError):
            self.advance(snap, expect)
        meta, rows = dump_ledger(self.state)
        if expect == 0:
            self.assertEqual(meta, [])
        self.assertEqual(rows, {})

    def test_missing_sales_table(self):
        self.assert_rejected_cleanly(self.publish(drop_sales=True))

    def test_missing_stores_table(self):
        self.assert_rejected_cleanly(self.publish(drop_stores=True))

    def test_wrong_column_type(self):
        bad_schema = [("day", "int64"), ("store", "text"), ("buyer", "text"),
                      ("amount", "text")]
        snap = self.publish(
            sales=[(1, {"day": 1, "store": "A", "buyer": "u",
                        "amount": "ten"})],
            stores=[(1, {"store": "A", "region": "N"})],
            sales_schema=bad_schema,
        )
        self.assert_rejected_cleanly(snap)

    def test_missing_column(self):
        bad_schema = [("day", "int64"), ("store", "text"),
                      ("amount", "int64")]
        snap = self.publish(
            sales=[(1, {"day": 1, "store": "A", "amount": 5})],
            stores=[(1, {"store": "A", "region": "N"})],
            sales_schema=bad_schema,
        )
        self.assert_rejected_cleanly(snap)

    def test_row_with_absent_cell(self):
        snap = self.publish(
            sales=[(1, sale(1, "A", "u", 10)),
                   (2, {"day": 1, "store": "A", "buyer": "v"})],  # no amount
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.assert_rejected_cleanly(snap)

    def test_row_with_null_cell(self):
        snap = self.publish(
            sales=[(1, sale(1, "A", "u", None))],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.assert_rejected_cleanly(snap)

    def test_null_region_row(self):
        snap = self.publish(
            sales=[(1, sale(1, "A", "u", 10))],
            stores=[(1, {"store": "A", "region": None})],
        )
        self.assert_rejected_cleanly(snap)

    def test_stores_without_lookup_key_declaration(self):
        snap = self.publish(
            sales=[(1, sale(1, "A", "u", 10))],
            stores=[(1, {"store": "A", "region": "N"})],
            stores_key=None,
        )
        self.assert_rejected_cleanly(snap)

    def test_rejection_leaves_no_partial_advance(self):
        good = self.good_snapshot()
        self.advance(good, 0)
        bad = self.publish(
            sales=[(1, sale(1, "A", "u", 10)),
                   (2, {"day": 2, "store": "B", "buyer": "v"})],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        with self.assertRaises(RollupContractError):
            self.advance(bad, 1)
        # The failed advance did not move the ledger at all.
        self.assertLedger(1, good)
        # ... and the ledger can keep advancing from the consistent revision.
        good2 = self.publish(
            sales=[(1, sale(1, "A", "u", 40))],
            stores=[(1, {"store": "A", "region": "S"})],
        )
        self.advance(good2, 1)
        self.assertLedger(2, good2)

    def test_unknown_snapshot_id(self):
        with self.assertRaises(RollupError):
            advance_sales_rollup(self.registry, "no-such-snapshot",
                                 self.state, 0)

    def test_strings_match_verbatim(self):
        # "n" and "N" are different regions; no trimming or case folding.
        snap = self.publish(
            sales=[(1, sale(1, "A", "u", 1)), (2, sale(1, "B", "u", 2)),
                   (3, sale(1, " A", "u", 4))],
            stores=[(1, {"store": "A", "region": "N"}),
                    (2, {"store": "B", "region": "n"})],
        )
        self.advance(snap, 0)
        self.assertLedger(1, snap, expected={
            (1, "N"): (1, 1),
            (1, "n"): (2, 1),
            (1, None): (4, 1),  # " A" is not the registered "A"
        })


class RevisionProtocolTests(RollupTestBase):
    def test_revision_flow_and_retry_exception(self):
        snap1 = self.publish(sales=[(1, sale(1, "A", "u", 1))],
                             stores=[(1, {"store": "A", "region": "N"})])
        snap2 = self.publish(sales=[(1, sale(1, "A", "u", 2))],
                             stores=[(1, {"store": "A", "region": "S"})])

        # Empty state only accepts --expect 0.
        with self.assertRaises(RollupRevisionError):
            self.advance(snap1, 1)

        first = self.advance(snap1, 0)
        self.assertEqual(first.revision, 1)

        # The sole retry exception: same snapshot, expect one behind.
        retry = self.advance(snap1, 0)
        self.assertFalse(retry.advanced)
        self.assertEqual(retry.revision, 1)
        self.assertLedger(1, snap1)

        # Same expect with a different target is stale, not a retry.
        with self.assertRaises(RollupRevisionError):
            self.advance(snap2, 0)
        # Skipping ahead is stale too.
        with self.assertRaises(RollupRevisionError):
            self.advance(snap2, 5)

        second = self.advance(snap2, 1)
        self.assertEqual(second.revision, 2)
        self.assertLedger(2, snap2)

        # A result-identical advance still commits one revision forward.
        third = self.advance(snap2, 2)
        self.assertEqual(third.revision, 3)
        self.assertLedger(3, snap2)

    def test_two_operators_same_old_revision_only_one_wins(self):
        snap_a = self.publish(sales=[(1, sale(1, "A", "u", 1))],
                              stores=[(1, {"store": "A", "region": "N"})],
                              name="op-base")
        snap_b = self.publish(sales=[(1, sale(1, "A", "u", 10))],
                              stores=[(1, {"store": "A", "region": "N"})],
                              name="op-b")
        snap_c = self.publish(sales=[(1, sale(1, "A", "u", 99))],
                              stores=[(1, {"store": "A", "region": "S"})],
                              name="op-c")
        self.advance(snap_a, 0)
        # Both operators advance from revision 1 to different targets.
        self.advance(snap_b, 1)
        with self.assertRaises(RollupRevisionError):
            self.advance(snap_c, 1)
        self.assertLedger(2, snap_b)
        # The loser retries from the now-current revision and lands.
        result = self.advance(snap_c, 2)
        self.assertEqual(result.revision, 3)
        self.assertLedger(3, snap_c)


class OverflowTests(RollupTestBase):
    def test_final_gross_beyond_int64_rejects_whole_advance(self):
        snap = self.publish(
            sales=[(1, sale(1, "A", "u", 2**62)),
                   (2, sale(1, "A", "v", 2**62))],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        with self.assertRaises(RollupOverflowError):
            self.advance(snap, 0)
        meta, rows = dump_ledger(self.state)
        self.assertEqual(meta, [])
        self.assertEqual(rows, {})
        # A legal follow-up still initialises the ledger.
        good = self.publish(sales=[(1, sale(1, "A", "u", 5))],
                            stores=[(1, {"store": "A", "region": "N"})])
        self.advance(good, 0)
        self.assertLedger(1, good)

    def test_intermediate_overflow_with_legal_final_is_accepted(self):
        # Buyer u's subtotal is 2**63 (overflows int64 if summed in SQL),
        # yet the final group gross is 1.
        snap = self.publish(
            sales=[(1, sale(1, "A", "u", 2**62)),
                   (2, sale(1, "A", "u", 2**62)),
                   (3, sale(1, "A", "v", INT64_MIN + 1))],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.advance(snap, 0)
        self.assertLedger(1, snap, expected={(1, "N"): (1, 2)})

    def test_extreme_amounts_and_delta_beyond_int64(self):
        snap1 = self.publish(
            sales=[(1, sale(1, "A", "u", INT64_MIN))],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.advance(snap1, 0)
        self.assertLedger(1, snap1, expected={(1, "N"): (INT64_MIN, 1)})
        # Replacing -2**63 with +2**63-1 produces a net delta of 2**64-1,
        # itself beyond int64, while the final gross is legal.
        snap2 = self.publish(
            sales=[(2, sale(1, "A", "u", INT64_MAX))],
            stores=[(1, {"store": "A", "region": "N"})],
        )
        self.advance(snap2, 1)
        self.assertLedger(2, snap2, expected={(1, "N"): (INT64_MAX, 1)})


class CliTests(RollupTestBase):
    def cli(self, *args):
        return subprocess.run(
            [sys.executable, "-I", ENTRY, *args],
            cwd=PROJECT, capture_output=True, text=True, check=False,
        )

    def rollup_args(self, snapshot, expect):
        return ["advance-sales-rollup",
                "--registry", self.registry.root,
                "--snapshot-id", snapshot.snapshot_id,
                "--state", self.state,
                "--expect", str(expect)]

    def test_cli_success_retry_and_stale(self):
        snap1 = self.publish(sales=[(1, sale(1, "A", "u", 3))],
                             stores=[(1, {"store": "A", "region": "N"})])
        snap2 = self.publish(sales=[(1, sale(1, "A", "u", 4))],
                             stores=[(1, {"store": "A", "region": "N"})])
        proc = self.cli(*self.rollup_args(snap1, 0))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertEqual(out["revision"], 1)
        self.assertEqual(out["snapshot_id"], snap1.snapshot_id)
        self.assertEqual(out["snapshot_digest"], snap1.digest)
        self.assertTrue(out["advanced"])

        # Committed retry: exit 0, no new write.
        proc = self.cli(*self.rollup_args(snap1, 0))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertEqual(out["revision"], 1)
        self.assertFalse(out["advanced"])

        # Stale expect with a different target: exit 3.
        proc = self.cli(*self.rollup_args(snap2, 0))
        self.assertEqual(proc.returncode, 3, proc.stdout)
        # Correct expect: exit 0 and revision 2.
        proc = self.cli(*self.rollup_args(snap2, 1))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["revision"], 2)

    def test_cli_contract_violation_exit_code(self):
        snap = self.publish(drop_sales=True)
        proc = self.cli(*self.rollup_args(snap, 0))
        self.assertEqual(proc.returncode, 2, proc.stdout)
        self.assertIn("sales", proc.stderr)

    def test_cli_competing_processes_single_winner(self):
        snap_a = self.publish(sales=[(1, sale(1, "A", "u", 1))],
                              stores=[(1, {"store": "A", "region": "N"})],
                              name="cli-base")
        snap_b = self.publish(sales=[(1, sale(1, "A", "u", 10))],
                              stores=[(1, {"store": "A", "region": "N"})],
                              name="cli-b")
        snap_c = self.publish(sales=[(1, sale(1, "A", "u", 99))],
                              stores=[(1, {"store": "A", "region": "S"})],
                              name="cli-c")
        proc = self.cli(*self.rollup_args(snap_a, 0))
        self.assertEqual(proc.returncode, 0, proc.stderr)

        # Two operators race from revision 1 to different targets.
        procs = [
            self.cli(*self.rollup_args(snap_b, 1)),
            self.cli(*self.rollup_args(snap_c, 1)),
        ]
        codes = sorted(p.returncode for p in procs)
        self.assertEqual(codes, [0, 3], [p.stderr for p in procs])
        winner = snap_b if procs[0].returncode == 0 else snap_c
        loser = snap_c if procs[0].returncode == 0 else snap_b
        self.assertLedger(2, winner)

        # The loser resumes from the consistent revision and lands.
        proc = self.cli(*self.rollup_args(loser, 2))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["revision"], 3)
        self.assertLedger(3, loser)


if __name__ == "__main__":
    unittest.main()
