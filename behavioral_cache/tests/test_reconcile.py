"""reconcile-table: three-way merge of one lookup-keyed table.

Covers the acceptance scenarios from the specification:

* complementary changes (one branch fixes a code, the other a remark) are
  both kept;
* delete-vs-modify, same-id adds and null-vs-missing cases before and
  after review;
* the key-ownership check: base ``1:A, 2:B, 3:C`` with left moving 1->X
  and right moving 2->X lists 1 and 2 as competitors for X; ``1:X, 2:C``
  is rejected without touching anything; ``1:B, 2:X`` yields
  ``1:B, 2:X, 3:C``;
* a row/cell ruling changes later key ownership, and stale key rulings
  (old token) cannot be misused;
* many key conflicts still stream out line by line and merge with bounded
  memory (data generated on demand, never committed).
"""

import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bcache import (  # noqa: E402
    MISSING,
    CacheStore,
    PendingDecisions,
    ReconcileDecisionError,
    ReconcileError,
    ReconcileInputError,
    Reader,
    SnapshotBuilder,
    SnapshotRegistry,
    reconcile_table,
)
from bcache.snapshot import SnapshotConflict  # noqa: E402

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENTRY = os.path.join(PROJECT, "bcache_run.py")
RATE_JOB = os.path.join(PROJECT, "jobs", "rate_sales.py")

STORES_SCHEMA = [("code", "text"), ("remark", "text")]


class ReconcileBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.registry = SnapshotRegistry(os.path.join(self.tmp, "registry"))

    # -- construction -------------------------------------------------------

    def publish(self, dirname, sid, tables):
        """tables: list of (table_name, schema, key_col, {row_id: values}).

        Re-publishing an id replaces the fixture snapshot (test setup only;
        the product never replaces published content).
        """
        self._build_seq = getattr(self, "_build_seq", 0) + 1
        builder = SnapshotBuilder(
            os.path.join(self.tmp, "%s-%d" % (dirname, self._build_seq))
        )
        for tname, schema, key_col, rows in tables:
            builder.add_table(tname, schema, key_col=key_col)
            for rid, values in rows.items():
                builder.add_row(tname, rid, values)
        snap = builder.finalize(snapshot_id=sid)
        existing = os.path.join(self.registry.snapshots_dir, sid)
        if os.path.exists(existing):
            shutil.rmtree(existing)
        self.registry.publish(snap)
        return snap

    def publish_stores(self, sid, rows, extra_tables=()):
        tables = [("stores", STORES_SCHEMA, "code", rows)]
        tables.extend(extra_tables)
        return self.publish("dir-" + sid, sid, tables)

    # -- helpers ------------------------------------------------------------

    def stores_rows(self, snapshot):
        conn = snapshot.connect()
        try:
            return {
                rid: dict(values)
                for rid, values in snapshot.scan(conn, "stores")
            }
        finally:
            conn.close()

    def inputs_summary(self):
        summary = {}
        for side in ("base", "left", "right"):
            snap = self.registry.open(side)
            conn = snap.connect()
            try:
                rows = snap.count_rows(conn, "stores")
            finally:
                conn.close()
            summary[side] = {"id": side, "digest": snap.digest, "rows": rows}
        return summary

    def reconcile(self, output_id, decisions_path=None):
        buf = io.BytesIO()
        result = reconcile_table(
            self.registry, "base", "left", "right", "stores", output_id,
            decisions_path=decisions_path, pending_out=buf,
        )
        self.assertEqual(buf.getvalue(), b"", "success emits no JSONL")
        return result

    def pending(self, output_id, decisions_path=None, stage=None):
        buf = io.BytesIO()
        with self.assertRaises(PendingDecisions) as caught:
            reconcile_table(
                self.registry, "base", "left", "right", "stores", output_id,
                decisions_path=decisions_path, pending_out=buf,
            )
        if stage is not None:
            self.assertEqual(caught.exception.stage, stage)
        lines = [
            json.loads(x)
            for x in buf.getvalue().decode("utf-8").splitlines()
        ]
        self.assertEqual(lines[0]["kind"], "header")
        return lines

    def write_decisions(self, decisions, token=None, inputs=None,
                        name="decisions.jsonl", header_extra=None):
        path = os.path.join(self.tmp, name)
        header = {
            "kind": "header",
            "table": "stores",
            "inputs": inputs if inputs is not None else self.inputs_summary(),
        }
        if token is not None:
            header["token"] = token
        if header_extra:
            header.update(header_extra)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(header, ensure_ascii=False) + "\n")
            for decision in decisions:
                fh.write(json.dumps(decision, ensure_ascii=False) + "\n")
        return path

    def corrupt(self, snapshot, sql, params=()):
        conn = sqlite3.connect(snapshot.db_path)
        try:
            conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()


class ComplementaryChangesTests(ReconcileBase):
    """One branch changes a row's code, the other the same row's remark."""

    def setUp(self):
        super().setUp()
        self.base = self.publish_stores("base", {
            1: {"code": "A", "remark": "r1"},
            2: {"code": "B", "remark": "r2"},
        }, extra_tables=[
            ("other", [("k", "text"), ("n", "int64")], "k",
             {7: {"k": "o", "n": 1}}),
            ("empty", [("x", "text")], None, {}),
        ])
        self.left = self.publish_stores("left", {
            1: {"code": "X", "remark": "r1"},   # code corrected by team A
            2: {"code": "B", "remark": "r2"},
        })
        self.right = self.publish_stores("right", {
            1: {"code": "A", "remark": "r1b"},  # remark completed by team B
            2: {"code": "B", "remark": "r2"},
        })

    def test_both_changes_are_kept(self):
        result = self.reconcile("merged")
        rows = self.stores_rows(result.snapshot)
        self.assertEqual(rows[1], {"code": "X", "remark": "r1b"})
        self.assertEqual(rows[2], {"code": "B", "remark": "r2"})
        self.assertEqual(result.rows, 2)
        self.assertEqual(
            (result.row_conflicts, result.cell_conflicts,
             result.key_conflicts, result.decisions_applied),
            (0, 0, 0, 0),
        )

    def test_other_tables_come_from_base_only(self):
        result = self.reconcile("merged")
        snap = result.snapshot
        conn = snap.connect()
        try:
            self.assertEqual(snap.table_names(conn),
                             ["empty", "other", "stores"])
            self.assertEqual(snap.key_col(conn, "other"), "k")
            self.assertEqual(snap.count_rows(conn, "empty"), 0)
            self.assertEqual(
                {rid: dict(v) for rid, v in snap.scan(conn, "other")},
                {7: {"k": "o", "n": 1}},
            )
        finally:
            conn.close()

    def test_inputs_current_and_digest_stability(self):
        current_before = self.registry.current_id()
        digests_before = {
            sid: self.registry.open(sid).digest
            for sid in ("base", "left", "right")
        }
        first = self.reconcile("merged")
        again = self.reconcile("merged")  # same content: idempotent retry
        self.assertEqual(first.outcome, "published")
        self.assertEqual(again.outcome, "unchanged")
        elsewhere = self.reconcile("merged-elsewhere")
        self.assertEqual(elsewhere.snapshot.digest, first.snapshot.digest)
        self.assertEqual(self.registry.current_id(), current_before)
        for sid, digest in digests_before.items():
            self.assertEqual(self.registry.open(sid).digest, digest)

    def test_same_id_different_content_refused(self):
        self.reconcile("fixed")
        # Change the right side, re-merge under the same id: different
        # content must be refused and the published snapshot kept.
        self.publish_stores("right", {
            1: {"code": "A", "remark": "changed-again"},
            2: {"code": "B", "remark": "r2"},
        })
        with self.assertRaises(SnapshotConflict):
            self.reconcile("fixed")
        rows = self.stores_rows(self.registry.open("fixed"))
        self.assertEqual(rows[1]["remark"], "r1b")

    def test_identical_change_on_both_sides_kept_once(self):
        self.publish_stores("left", {
            1: {"code": "X", "remark": "r1"},
            2: {"code": "B", "remark": "r2"},
        })
        self.publish_stores("right", {
            1: {"code": "X", "remark": "r1"},
            2: {"code": "B", "remark": "r2"},
        })
        result = self.reconcile("merged")
        self.assertEqual(result.row_conflicts + result.cell_conflicts, 0)
        self.assertEqual(
            self.stores_rows(result.snapshot)[1],
            {"code": "X", "remark": "r1"},
        )

    def test_no_changes_anywhere_yields_base_content(self):
        self.publish_stores("left", {
            1: {"code": "A", "remark": "r1"},
            2: {"code": "B", "remark": "r2"},
        })
        self.publish_stores("right", {
            1: {"code": "A", "remark": "r1"},
            2: {"code": "B", "remark": "r2"},
        })
        result = self.reconcile("merged")
        self.assertEqual(result.snapshot.digest, self.base.digest)
        # A header-only decisions file is a legal no-op submission.
        result = self.reconcile("merged-2", self.write_decisions([]))
        self.assertEqual(result.snapshot.digest, self.base.digest)


class DeleteModifyTests(ReconcileBase):
    def setUp(self):
        super().setUp()
        self.publish_stores("base", {
            1: {"code": "A", "remark": "r1"},
            2: {"code": "B", "remark": "r2"},
            3: {"code": "C", "remark": "r3"},
            4: {"code": "D", "remark": "r4"},
        })
        # left deletes row 2, modifies row 3, deletes row 4
        self.publish_stores("left", {
            1: {"code": "A", "remark": "r1"},
            3: {"code": "C", "remark": "r3L"},
        })
        # right modifies row 2, deletes row 3, deletes row 4
        self.publish_stores("right", {
            1: {"code": "A", "remark": "r1"},
            2: {"code": "B", "remark": "r2R"},
        })

    def test_pending_listing_shows_row_conflicts_only(self):
        lines = self.pending("merged", stage="rows")
        header, records = lines[0], lines[1:]
        self.assertEqual(header["stage"], "rows")
        self.assertEqual(header["table"], "stores")
        self.assertEqual(
            [s["id"] for s in (header["inputs"][k]
                               for k in ("base", "left", "right"))],
            ["base", "left", "right"],
        )
        # Row conflicts: 2 (delete-left vs modify-right) and 3 (modify-left
        # vs delete-right).  Row 4 is deleted on both sides: no conflict,
        # and row conflicts are not broken down into cell conflicts.
        self.assertEqual([r["row_id"] for r in records], [2, 3])
        self.assertTrue(all(r["conflict"] == "row" for r in records))
        rec2, rec3 = records
        self.assertEqual(rec2["left"], {"state": "absent"})
        self.assertEqual(rec2["right"]["state"], "present")
        self.assertEqual(
            rec2["right"]["cells"]["remark"],
            {"state": "value", "value": "r2R"},
        )
        self.assertEqual(rec2["base"]["state"], "present")
        self.assertEqual(rec3["right"], {"state": "absent"})
        self.assertEqual(rec3["left"]["state"], "present")

    def test_rulings_pick_whole_side_or_deletion(self):
        decisions = self.write_decisions([
            {"kind": "decision", "conflict": "row", "row_id": 2,
             "side": "right"},
            {"kind": "decision", "conflict": "row", "row_id": 3,
             "side": "left"},
        ])
        result = self.reconcile("merged", decisions)
        rows = self.stores_rows(result.snapshot)
        self.assertEqual(set(rows), {1, 2, 3})
        self.assertEqual(rows[2], {"code": "B", "remark": "r2R"})
        self.assertEqual(rows[3], {"code": "C", "remark": "r3L"})
        self.assertEqual(result.row_conflicts, 2)
        self.assertEqual(result.decisions_applied, 2)

    def test_ruling_for_the_deleting_side_drops_the_row(self):
        decisions = self.write_decisions([
            {"kind": "decision", "conflict": "row", "row_id": 2,
             "side": "left"},
            {"kind": "decision", "conflict": "row", "row_id": 3,
             "side": "right"},
        ])
        result = self.reconcile("merged", decisions)
        self.assertEqual(set(self.stores_rows(result.snapshot)), {1})

    def test_partial_rulings_list_only_what_remains(self):
        decisions = self.write_decisions([
            {"kind": "decision", "conflict": "row", "row_id": 2,
             "side": "right"},
        ])
        lines = self.pending("merged", decisions, stage="rows")
        self.assertEqual([r["row_id"] for r in lines[1:]], [3])

    def test_delete_vs_unchanged_and_double_delete_are_silent(self):
        # Row 4 is deleted on both sides; add row 5 deleted only on the
        # left and untouched on the right: both drop out without conflicts.
        self.publish_stores("base", {
            1: {"code": "A", "remark": "r1"},
            4: {"code": "D", "remark": "r4"},
            5: {"code": "E", "remark": "r5"},
        })
        self.publish_stores("left", {
            1: {"code": "A", "remark": "r1"},
        })
        self.publish_stores("right", {
            1: {"code": "A", "remark": "r1"},
            5: {"code": "E", "remark": "r5"},
        })
        result = self.reconcile("merged")
        self.assertEqual(set(self.stores_rows(result.snapshot)), {1})
        self.assertEqual(result.row_conflicts, 0)


class BothAddTests(ReconcileBase):
    def setUp(self):
        super().setUp()
        self.publish_stores("base", {
            1: {"code": "A", "remark": "r1"},
        })

    def test_same_row_id_added_with_different_content_conflicts(self):
        self.publish_stores("left", {
            1: {"code": "A", "remark": "r1"},
            9: {"code": "N", "remark": "from-left"},
        })
        self.publish_stores("right", {
            1: {"code": "A", "remark": "r1"},
            9: {"code": "N", "remark": "from-right"},
        })
        lines = self.pending("merged", stage="rows")
        records = lines[1:]
        self.assertEqual(len(records), 1)
        rec = records[0]
        self.assertEqual(rec["conflict"], "row")
        self.assertEqual(rec["row_id"], 9)
        self.assertEqual(rec["base"], {"state": "absent"})
        self.assertEqual(rec["left"]["cells"]["remark"],
                         {"state": "value", "value": "from-left"})
        self.assertEqual(rec["right"]["cells"]["remark"],
                         {"state": "value", "value": "from-right"})
        # The two new rows are never spliced: the ruling picks one side.
        decisions = self.write_decisions([
            {"kind": "decision", "conflict": "row", "row_id": 9,
             "side": "right"},
        ])
        result = self.reconcile("merged", decisions)
        self.assertEqual(
            self.stores_rows(result.snapshot)[9],
            {"code": "N", "remark": "from-right"},
        )

    def test_identical_add_kept_once_and_single_side_add_adopted(self):
        self.publish_stores("left", {
            1: {"code": "A", "remark": "r1"},
            9: {"code": "N", "remark": "same"},
            10: {"code": "L", "remark": "left-only"},
        })
        self.publish_stores("right", {
            1: {"code": "A", "remark": "r1"},
            9: {"code": "N", "remark": "same"},
            11: {"code": "R", "remark": "right-only"},
        })
        result = self.reconcile("merged")
        rows = self.stores_rows(result.snapshot)
        self.assertEqual(set(rows), {1, 9, 10, 11})
        self.assertEqual(rows[9], {"code": "N", "remark": "same"})
        self.assertEqual(rows[10]["code"], "L")
        self.assertEqual(rows[11]["code"], "R")
        self.assertEqual(result.row_conflicts, 0)


class NullMissingTests(ReconcileBase):
    """A missing cell is not an explicit NULL, in diffs and in output."""

    def setUp(self):
        super().setUp()
        self.publish_stores("base", {
            1: {"code": "A"},                          # remark missing
            2: {"code": "B", "remark": None},          # explicit NULL
            3: {"code": "C", "remark": None},          # NULL -> conflict
        })
        self.publish_stores("left", {
            1: {"code": "A", "remark": "m"},           # fills missing cell
            2: {"code": "B", "remark": None},
            3: {"code": "C"},                          # NULL -> missing
        })
        self.publish_stores("right", {
            1: {"code": "A"},
            2: {"code": "B", "remark": "v"},           # NULL -> value
            3: {"code": "C", "remark": "w"},           # NULL -> value
        })

    def test_one_sided_null_and_missing_changes_are_adopted(self):
        lines = self.pending("merged", stage="rows")
        records = lines[1:]
        # Only row 3 conflicts: base NULL, left missing, right "w".
        self.assertEqual(len(records), 1)
        rec = records[0]
        self.assertEqual((rec["conflict"], rec["row_id"], rec["column"]),
                         ("cell", 3, "remark"))
        self.assertEqual(rec["base"], {"state": "null"})
        self.assertEqual(rec["left"], {"state": "missing"})
        self.assertEqual(rec["right"], {"state": "value", "value": "w"})

    def test_ruling_missing_vs_value(self):
        decisions = self.write_decisions([
            {"kind": "decision", "conflict": "cell", "row_id": 3,
             "column": "remark", "side": "left"},
        ])
        result = self.reconcile("merged", decisions)
        rows = self.stores_rows(result.snapshot)
        self.assertEqual(rows[1]["remark"], "m")       # left's fill kept
        self.assertEqual(rows[2]["remark"], "v")       # right's fill kept
        self.assertIs(rows[3]["remark"], MISSING)      # missing, not NULL

    def test_ruling_value_over_missing(self):
        decisions = self.write_decisions([
            {"kind": "decision", "conflict": "cell", "row_id": 3,
             "column": "remark", "side": "right"},
        ])
        result = self.reconcile("merged", decisions)
        self.assertEqual(
            self.stores_rows(result.snapshot)[3]["remark"], "w")

    def test_null_survives_when_only_one_side_changes_it(self):
        # Base NULL, left NULL (unchanged), right value -> right adopted;
        # and base NULL vs left value vs right NULL is a conflict whose
        # "null" side is shown distinctly from "missing".
        self.publish_stores("base", {1: {"code": "A", "remark": None}})
        self.publish_stores("left", {1: {"code": "A", "remark": "x"}})
        self.publish_stores("right", {1: {"code": "A", "remark": None}})
        result = self.reconcile("merged")
        self.assertEqual(
            self.stores_rows(result.snapshot)[1]["remark"], "x")


class KeyConflictAcceptanceTests(ReconcileBase):
    """The specification's worked example: 1:A 2:B 3:C, 1->X, 2->X."""

    def setUp(self):
        super().setUp()
        self.publish_stores("base", {
            1: {"code": "A"}, 2: {"code": "B"}, 3: {"code": "C"},
        })
        self.publish_stores("left", {
            1: {"code": "X"}, 2: {"code": "B"}, 3: {"code": "C"},
        })
        self.publish_stores("right", {
            1: {"code": "A"}, 2: {"code": "X"}, 3: {"code": "C"},
        })

    def test_pending_lists_both_rows_as_competitors_for_x(self):
        lines = self.pending("merged", stage="keys")
        header, records = lines[0], lines[1:]
        self.assertEqual(header["stage"], "keys")
        self.assertIn("token", header)
        self.assertEqual(
            [(r["key"], r["row_id"]) for r in records],
            [("X", 1), ("X", 2)],
        )

    def test_ruling_colliding_with_uninvolved_row_is_rejected(self):
        header = self.pending("merged", stage="keys")[0]
        decisions = self.write_decisions(
            [
                {"kind": "decision", "conflict": "key", "row_id": 1,
                 "key": "X"},
                {"kind": "decision", "conflict": "key", "row_id": 2,
                 "key": "C"},
            ],
            token=header["token"],
        )
        before = {
            sid: self.registry.open(sid).digest
            for sid in ("base", "left", "right")
        }
        with self.assertRaises(ReconcileDecisionError):
            self.reconcile("merged", decisions)
        # Nothing was published and the inputs are untouched.
        self.assertFalse(os.path.exists(
            os.path.join(self.registry.snapshots_dir, "merged")))
        for sid, digest in before.items():
            self.assertEqual(self.registry.open(sid).digest, digest)

    def test_ruling_hands_key_between_rows_and_publishes(self):
        header = self.pending("merged", stage="keys")[0]
        decisions = self.write_decisions(
            [
                {"kind": "decision", "conflict": "key", "row_id": 1,
                 "key": "B"},
                {"kind": "decision", "conflict": "key", "row_id": 2,
                 "key": "X"},
            ],
            token=header["token"],
        )
        result = self.reconcile("merged", decisions)
        rows = self.stores_rows(result.snapshot)
        self.assertEqual(
            {rid: row["code"] for rid, row in rows.items()},
            {1: "B", 2: "X", 3: "C"},
        )
        self.assertEqual(result.key_conflicts, 1)
        self.assertEqual(result.decisions_applied, 2)

    def test_incomplete_key_rulings_relist_the_contested_key(self):
        header = self.pending("merged", stage="keys")[0]
        decisions = self.write_decisions(
            [{"kind": "decision", "conflict": "key", "row_id": 1,
              "key": "B"}],
            token=header["token"],
        )
        lines = self.pending("merged", decisions, stage="keys")
        # The key is still contested; all its participants are listed.
        self.assertEqual(
            [(r["key"], r["row_id"]) for r in lines[1:]],
            [("X", 1), ("X", 2)],
        )

    def test_added_row_can_contest_a_key(self):
        # Left adds row 9 with code X while right moves row 2 to X: the
        # added row participates in the key conflict like any other.
        self.publish_stores("left", {
            1: {"code": "A"}, 2: {"code": "B"}, 3: {"code": "C"},
            9: {"code": "X"},
        })
        lines = self.pending("merged", stage="keys")
        self.assertEqual(
            [(r["key"], r["row_id"]) for r in lines[1:]],
            [("X", 2), ("X", 9)],
        )
        header = lines[0]
        decisions = self.write_decisions(
            [
                {"kind": "decision", "conflict": "key", "row_id": 2,
                 "key": "B"},
                {"kind": "decision", "conflict": "key", "row_id": 9,
                 "key": "X"},
            ],
            token=header["token"],
        )
        result = self.reconcile("merged", decisions)
        self.assertEqual(
            {rid: row["code"]
             for rid, row in self.stores_rows(result.snapshot).items()},
            {1: "A", 2: "B", 3: "C", 9: "X"},
        )


class TokenBindingTests(ReconcileBase):
    """A row/cell ruling changes later key ownership; stale tokens fail."""

    def setUp(self):
        super().setUp()
        self.publish_stores("base", {
            1: {"code": "A"}, 2: {"code": "B"}, 3: {"code": "C"},
        })
        self.publish_stores("left", {
            1: {"code": "X"}, 2: {"code": "B"}, 3: {"code": "C"},
        })
        self.publish_stores("right", {
            1: {"code": "Y"}, 2: {"code": "X"}, 3: {"code": "C"},
        })
        # Row 1's code is a cell conflict (X vs Y).  Row 2 only changed on
        # the right (B -> X).  Ruling row 1 "left" makes keys X, X, C (a
        # key conflict); ruling "right" makes Y, X, C (no conflict).

    def _cell_ruling(self, side):
        return [{"kind": "decision", "conflict": "cell", "row_id": 1,
                 "column": "code", "side": side}]

    def test_cell_ruling_determines_whether_keys_conflict(self):
        # Ruling "left" surfaces a key conflict on X between rows 1 and 2.
        decisions = self.write_decisions(self._cell_ruling("left"))
        lines = self.pending("merged", decisions, stage="keys")
        self.assertEqual(
            [(r["key"], r["row_id"]) for r in lines[1:]],
            [("X", 1), ("X", 2)],
        )
        # Ruling "right" produces no key conflict at all.
        decisions = self.write_decisions(self._cell_ruling("right"),
                                         name="d2.jsonl")
        result = self.reconcile("merged", decisions)
        self.assertEqual(
            {rid: row["code"]
             for rid, row in self.stores_rows(result.snapshot).items()},
            {1: "Y", 2: "X", 3: "C"},
        )

    def test_old_token_cannot_be_reused_after_changing_row_ruling(self):
        decisions = self.write_decisions(self._cell_ruling("left"))
        header = self.pending("merged", decisions, stage="keys")[0]
        stale_token = header["token"]
        key_rulings = [
            {"kind": "decision", "conflict": "key", "row_id": 1,
             "key": "X"},
            {"kind": "decision", "conflict": "key", "row_id": 2,
             "key": "Q"},
        ]
        # The same row ruling + token + key rulings succeeds.
        good = self.write_decisions(
            self._cell_ruling("left") + key_rulings,
            token=stale_token, name="good.jsonl")
        result = self.reconcile("merged", good)
        self.assertEqual(
            {rid: row["code"]
             for rid, row in self.stores_rows(result.snapshot).items()},
            {1: "X", 2: "Q", 3: "C"},
        )
        # Changing the earlier cell ruling but keeping the old token and
        # old key rulings is rejected and publishes nothing.
        bad = self.write_decisions(
            self._cell_ruling("right") + key_rulings,
            token=stale_token, name="bad.jsonl")
        with self.assertRaises(ReconcileDecisionError):
            self.reconcile("merged-2", bad)
        self.assertFalse(os.path.exists(
            os.path.join(self.registry.snapshots_dir, "merged-2")))

    def test_key_rulings_require_the_token(self):
        decisions = self.write_decisions(
            self._cell_ruling("left") + [
                {"kind": "decision", "conflict": "key", "row_id": 1,
                 "key": "X"},
                {"kind": "decision", "conflict": "key", "row_id": 2,
                 "key": "Q"},
            ])  # no token in the header
        with self.assertRaises(ReconcileDecisionError):
            self.reconcile("merged", decisions)

    def test_reordered_rulings_keep_the_token_valid(self):
        decisions = self.write_decisions(self._cell_ruling("left"))
        header = self.pending("merged", decisions, stage="keys")[0]
        # Two cell conflicts would allow reordering; emulate by writing the
        # same rulings in a different file order (key rulings reversed).
        reordered = self.write_decisions(
            [
                {"kind": "decision", "conflict": "key", "row_id": 2,
                 "key": "Q"},
                {"kind": "decision", "conflict": "cell", "row_id": 1,
                 "column": "code", "side": "left"},
                {"kind": "decision", "conflict": "key", "row_id": 1,
                 "key": "X"},
            ],
            token=header["token"], name="reordered.jsonl")
        result = self.reconcile("merged", reordered)
        self.assertEqual(result.rows, 3)


class DecisionValidationTests(ReconcileBase):
    def setUp(self):
        super().setUp()
        self.publish_stores("base", {
            1: {"code": "A", "remark": "r1"},
            2: {"code": "B", "remark": "r2"},
        })
        self.publish_stores("left", {
            1: {"code": "A", "remark": "r1"},
            2: {"code": "B", "remark": "L"},
        })
        self.publish_stores("right", {
            1: {"code": "A", "remark": "r1"},
            2: {"code": "B", "remark": "R"},
        })
        # One cell conflict: row 2 remark (L vs R).

    def _reject(self, decisions, **kwargs):
        path = self.write_decisions(decisions, **kwargs)
        with self.assertRaises(ReconcileDecisionError):
            self.reconcile("merged", path)
        self.assertFalse(os.path.exists(
            os.path.join(self.registry.snapshots_dir, "merged")))

    def _cell(self, side="left", row_id=2, column="remark"):
        return {"kind": "decision", "conflict": "cell", "row_id": row_id,
                "column": column, "side": side}

    def test_duplicate_ruling_rejected(self):
        self._reject([self._cell(), self._cell()])
        self._reject([self._cell("left"), self._cell("right")])

    def test_ruling_naming_no_conflict_rejected(self):
        self._reject([self._cell(row_id=9)])
        self._reject([self._cell(column="code")])
        self._reject([{"kind": "decision", "conflict": "row", "row_id": 2,
                       "side": "left"}])
        self._reject([{"kind": "decision", "conflict": "key", "row_id": 2,
                       "key": "Q"}])

    def test_wrong_types_rejected(self):
        self._reject([self._cell(side="middle")])
        self._reject([self._cell(side=1)])
        bad_row_id = self._cell()
        bad_row_id["row_id"] = True
        self._reject([bad_row_id])
        bad_row_id = self._cell()
        bad_row_id["row_id"] = "2"
        self._reject([bad_row_id])
        bad_key = {"kind": "decision", "conflict": "key", "row_id": 2,
                   "key": 5}
        self._reject([self._cell(), bad_key])
        unknown_field = self._cell()
        unknown_field["speed"] = "fast"
        self._reject([unknown_field])
        missing_field = self._cell()
        del missing_field["side"]
        self._reject([missing_field])

    def test_header_context_mismatch_rejected(self):
        inputs = self.inputs_summary()
        self._reject([self._cell()], inputs={**inputs, "table": "x"},
                     header_extra=None)
        tampered = self.inputs_summary()
        tampered["left"]["digest"] = "0" * 64
        self._reject([self._cell()], inputs=tampered)
        tampered = self.inputs_summary()
        tampered["base"]["rows"] = 99
        self._reject([self._cell()], inputs=tampered)
        self._reject([self._cell()], header_extra={"surprise": 1})

    def test_header_table_mismatch_rejected(self):
        path = os.path.join(self.tmp, "wrong-table.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "kind": "header", "table": "other",
                "inputs": self.inputs_summary()}) + "\n")
        with self.assertRaises(ReconcileDecisionError):
            self.reconcile("merged", path)

    def test_key_rulings_while_row_stage_unresolved_rejected(self):
        self._reject([{"kind": "decision", "conflict": "key", "row_id": 1,
                       "key": "Q"}])

    def test_token_while_row_stage_unresolved_rejected(self):
        self._reject([], token="f" * 64)

    def test_empty_and_headerless_files_rejected(self):
        empty = os.path.join(self.tmp, "empty.jsonl")
        open(empty, "w").close()
        with self.assertRaises(ReconcileDecisionError):
            self.reconcile("merged", empty)
        no_header = os.path.join(self.tmp, "no-header.jsonl")
        with open(no_header, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(self._cell()) + "\n")
        with self.assertRaises(ReconcileDecisionError):
            self.reconcile("merged", no_header)
        with self.assertRaises(ReconcileDecisionError):
            self.reconcile("merged", os.path.join(self.tmp, "missing.jsonl"))

    def test_malformed_json_and_duplicate_members_rejected(self):
        path = os.path.join(self.tmp, "bad.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write('{"kind": "header", "table": "stores"\n')
        with self.assertRaises(ReconcileDecisionError):
            self.reconcile("merged", path)
        header = json.dumps({"kind": "header", "table": "stores",
                             "inputs": self.inputs_summary()})
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(header + "\n")
            fh.write('{"kind": "decision", "kind": "decision"}\n')
        with self.assertRaises(ReconcileDecisionError):
            self.reconcile("merged", path)


class PreconditionTests(ReconcileBase):
    def _standard(self):
        self.publish_stores("base", {1: {"code": "A", "remark": "r"}})
        self.publish_stores("left", {1: {"code": "A", "remark": "r"}})
        self.publish_stores("right", {1: {"code": "A", "remark": "r"}})

    def test_schema_order_and_type_must_match(self):
        self._standard()
        self.publish("dir-right2", "right", [
            ("stores", [("remark", "text"), ("code", "text")], "code",
             {1: {"code": "A", "remark": "r"}}),
        ])
        with self.assertRaises(ReconcileInputError):
            self.reconcile("merged")
        self.publish("dir-right3", "right", [
            ("stores", [("code", "text"), ("remark", "int64")], "code",
             {1: {"code": "A", "remark": 3}}),
        ])
        with self.assertRaises(ReconcileInputError):
            self.reconcile("merged")

    def test_key_declaration_must_match_and_be_text(self):
        self._standard()
        self.publish("dir-right2", "right", [
            ("stores", STORES_SCHEMA, None,
             {1: {"code": "A", "remark": "r"}}),
        ])
        with self.assertRaises(ReconcileInputError):
            self.reconcile("merged")
        # A non-text lookup key is out of scope for this command.
        for sid in ("base", "left", "right"):
            self.publish("dir-" + sid + "-int", sid, [
                ("stores", [("code", "int64"), ("remark", "text")], "code",
                 {1: {"code": 7, "remark": "r"}}),
            ])
        with self.assertRaises(ReconcileInputError):
            self.reconcile("merged")
        # No declared key at all.
        for sid in ("base", "left", "right"):
            self.publish("dir-" + sid + "-nokey", sid, [
                ("stores", STORES_SCHEMA, None,
                 {1: {"code": "A", "remark": "r"}}),
            ])
        with self.assertRaises(ReconcileInputError):
            self.reconcile("merged")

    def test_table_must_exist_in_all_three(self):
        self._standard()
        self.publish("dir-left2", "left", [
            ("other", [("x", "text")], None, {}),
        ])
        with self.assertRaises(ReconcileInputError):
            self.reconcile("merged")

    def test_unknown_snapshot_ids_rejected(self):
        self._standard()
        with self.assertRaises(ReconcileInputError):
            reconcile_table(self.registry, "nope", "left", "right",
                            "stores", "merged", pending_out=io.BytesIO())

    def test_null_or_missing_keys_in_inputs_rejected(self):
        self._standard()
        right = self.registry.open("right")
        self.corrupt(
            right,
            "UPDATE cells SET value_type='null', int_value=NULL, "
            "text_value=NULL, bool_value=NULL "
            "WHERE table_name='stores' AND col_name='code' AND row_id=1",
        )
        with self.assertRaises(ReconcileInputError):
            self.reconcile("merged")
        self._standard()
        left = self.registry.open("left")
        self.corrupt(
            left,
            "DELETE FROM cells WHERE table_name='stores' "
            "AND col_name='code' AND row_id=1",
        )
        with self.assertRaises(ReconcileInputError):
            self.reconcile("merged")

    def test_duplicate_keys_within_one_input_rejected(self):
        self.publish_stores("base", {
            1: {"code": "A", "remark": "r"},
            2: {"code": "B", "remark": "s"},
        })
        self.publish_stores("left", {
            1: {"code": "A", "remark": "r"},
            2: {"code": "B", "remark": "s"},
        })
        self.publish_stores("right", {
            1: {"code": "A", "remark": "r"},
            2: {"code": "B", "remark": "s"},
        })
        left = self.registry.open("left")
        self.corrupt(
            left,
            "UPDATE cells SET text_value='A' WHERE table_name='stores' "
            "AND col_name='code' AND row_id=2",
        )
        with self.assertRaises(ReconcileInputError):
            self.reconcile("merged")

    def test_invalid_output_id_rejected(self):
        self._standard()
        with self.assertRaises(ReconcileError):
            reconcile_table(self.registry, "base", "left", "right",
                            "stores", "a/b", pending_out=io.BytesIO())


class OtherTablesAndEmptyTests(ReconcileBase):
    def test_branches_other_tables_are_never_merged(self):
        self.publish("dir-base", "base", [
            ("stores", STORES_SCHEMA, "code",
             {1: {"code": "A", "remark": "r"}}),
            ("other", [("k", "text"), ("n", "int64")], "k",
             {7: {"k": "o", "n": 1}}),
            ("empty", [("x", "text")], None, {}),
        ])
        self.publish("dir-left", "left", [
            ("stores", STORES_SCHEMA, "code",
             {1: {"code": "A", "remark": "rL"}}),
            ("other", [("k", "text"), ("n", "int64")], "k",
             {7: {"k": "o", "n": 999}}),          # changed on the branch
            ("extra", [("y", "text")], None, {1: {"y": "z"}}),
        ])
        self.publish("dir-right", "right", [
            ("stores", STORES_SCHEMA, "code",
             {1: {"code": "A", "remark": "r"}}),
            ("other", [("k", "text"), ("n", "int64")], "k", {}),
        ])
        result = self.reconcile("merged")
        snap = result.snapshot
        conn = snap.connect()
        try:
            self.assertEqual(snap.table_names(conn),
                             ["empty", "other", "stores"])
            self.assertEqual(
                {rid: dict(v) for rid, v in snap.scan(conn, "other")},
                {7: {"k": "o", "n": 1}})
            self.assertEqual(snap.count_rows(conn, "empty"), 0)
        finally:
            conn.close()

    def test_empty_table_everywhere_merges_to_empty(self):
        for sid in ("base", "left", "right"):
            self.publish_stores(sid, {})
        result = self.reconcile("merged")
        self.assertEqual(result.rows, 0)
        conn = result.snapshot.connect()
        try:
            self.assertEqual(result.snapshot.count_rows(conn, "stores"), 0)
        finally:
            conn.close()

    def test_all_rows_deleted_on_both_sides(self):
        self.publish_stores("base", {1: {"code": "A"}, 2: {"code": "B"}})
        self.publish_stores("left", {})
        self.publish_stores("right", {})
        result = self.reconcile("merged")
        self.assertEqual(result.rows, 0)


class JobHandoffTests(ReconcileBase):
    """The merged snapshot serves the existing jobs directly."""

    def test_rate_job_runs_on_merged_snapshot(self):
        self.publish("dir-base", "base", [
            ("rates", [("code", "text"), ("value", "int64")], "code",
             {1: {"code": "B", "value": 10}}),
            ("sales", [("region", "text"), ("amount", "int64")], None,
             {1: {"region": "N", "amount": 2},
              2: {"region": "N", "amount": 3}}),
        ])
        # Team A corrects the rate code B -> A; team B fills its value.
        self.publish("dir-left", "left", [
            ("rates", [("code", "text"), ("value", "int64")], "code",
             {1: {"code": "A", "value": 10}}),
        ])
        self.publish("dir-right", "right", [
            ("rates", [("code", "text"), ("value", "int64")], "code",
             {1: {"code": "B", "value": 4}}),
        ])
        buf = io.BytesIO()
        result = reconcile_table(
            self.registry, "base", "left", "right", "rates", "merged",
            pending_out=buf)
        rows = {rid: dict(v) for rid, v in result.snapshot.scan(
            result.snapshot.connect(), "rates")}
        self.assertEqual(rows[1], {"code": "A", "value": 4})

        store = CacheStore(os.path.join(self.tmp, "cache"))
        try:
            run = store.run(RATE_JOB, self.registry, snapshot_id="merged")
        finally:
            store.close()
        self.assertEqual(run.mode, "executed")
        self.assertEqual([dict(r) for r in run.output.rows],
                         [{"total": 20}])  # (2 + 3) * 4

        # The existing reader serves the merged snapshot too.
        snap = self.registry.open("merged")
        reader = Reader(snap, snap.connect())
        try:
            found = reader.lookup("rates", "A", key_col="code")
            self.assertEqual(found.status, "found")
            self.assertEqual(found.value["value"], 4)
        finally:
            reader.conn.close()


class ManyKeyConflictsTests(unittest.TestCase):
    """On-demand generated data: many key conflicts stream and merge."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _build_registry(self, groups):
        registry = SnapshotRegistry(
            os.path.join(self.tmp, "registry-%d" % groups)
        )
        n = groups * 2
        for side in ("base", "left", "right"):
            builder = SnapshotBuilder(
                os.path.join(self.tmp, "dir-%s-%d" % (side, groups))
            )
            builder.add_table("stores", STORES_SCHEMA, key_col="code")
            for rid in range(1, n + 1):
                g = (rid - 1) // 2
                code = "K%06d" % rid
                if side == "left" and rid % 2 == 1:
                    code = "D%06d" % g
                elif side == "right" and rid % 2 == 0:
                    code = "D%06d" % g
                builder.add_row("stores", rid,
                                {"code": code, "remark": "r"})
            registry.publish(builder.finalize(snapshot_id=side))
        return registry

    def _write_key_decisions(self, path, header, groups):
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "kind": "header", "table": "stores",
                "inputs": header["inputs"], "token": header["token"],
            }) + "\n")
            for g in range(groups):
                fh.write(json.dumps({
                    "kind": "decision", "conflict": "key",
                    "row_id": 2 * g + 1, "key": "D%06d" % g,
                }) + "\n")
                fh.write(json.dumps({
                    "kind": "decision", "conflict": "key",
                    "row_id": 2 * g + 2, "key": "E%06d" % g,
                }) + "\n")

    def test_many_key_conflicts_stream_and_merge(self):
        groups = 3000
        registry = self._build_registry(groups)
        listing = os.path.join(self.tmp, "listing.jsonl")
        with open(listing, "wb") as fh:
            with self.assertRaises(PendingDecisions):
                reconcile_table(
                    registry, "base", "left", "right", "stores", "merged",
                    pending_out=fh)
        with open(listing, "rb") as fh:
            first = json.loads(fh.readline())
            self.assertEqual(first["stage"], "keys")
            count = 1
            last = None
            for line in fh:
                last = json.loads(line)
                count += 1
        # One record per (contested key, participating row): 2 per group.
        self.assertEqual(count, 1 + 2 * groups)
        self.assertEqual((last["key"], last["row_id"]),
                         ("D%06d" % (groups - 1), 2 * groups))

        decisions = os.path.join(self.tmp, "decisions.jsonl")
        self._write_key_decisions(decisions, first, groups)
        result = reconcile_table(
            registry, "base", "left", "right", "stores", "merged",
            decisions_path=decisions, pending_out=io.BytesIO())
        self.assertEqual(result.rows, 2 * groups)
        self.assertEqual(result.key_conflicts, groups)
        self.assertEqual(result.decisions_applied, 2 * groups)
        conn = result.snapshot.connect()
        try:
            spot = {
                rid: dict(v)
                for rid, v in result.snapshot.scan(conn, "stores")
                if rid in (1, 2, 2 * groups - 1, 2 * groups)
            }
        finally:
            conn.close()
        self.assertEqual(spot[1]["code"], "D%06d" % 0)
        self.assertEqual(spot[2]["code"], "E%06d" % 0)
        self.assertEqual(spot[2 * groups - 1]["code"],
                         "D%06d" % (groups - 1))
        self.assertEqual(spot[2 * groups]["code"], "E%06d" % (groups - 1))

    def _peak_flow(self, groups):
        import tracemalloc

        registry = self._build_registry(groups)
        listing = os.path.join(self.tmp, "listing-%d.jsonl" % groups)
        decisions = os.path.join(self.tmp, "decisions-%d.jsonl" % groups)
        tracemalloc.start()
        try:
            with open(listing, "wb") as fh:
                try:
                    reconcile_table(
                        registry, "base", "left", "right", "stores",
                        "merged-%d" % groups, pending_out=fh)
                except PendingDecisions:
                    pass
            with open(listing, "rb") as fh:
                header = json.loads(fh.readline())
            self._write_key_decisions(decisions, header, groups)
            reconcile_table(
                registry, "base", "left", "right", "stores",
                "merged-%d" % groups, decisions_path=decisions,
                pending_out=io.BytesIO())
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        return peak

    def test_peak_memory_independent_of_rows_and_conflicts(self):
        small = self._peak_flow(800)
        large = self._peak_flow(3200)
        print("\n[reconcile] peak small=%d large=%d bytes" % (small, large))
        self.assertLess(large, small * 3 + 4 * 1024 * 1024,
                        msg="small=%d large=%d" % (small, large))
        self.assertLess(large, 32 * 1024 * 1024,
                        msg="peak %d" % large)


class CliTests(ReconcileBase):
    def setUp(self):
        super().setUp()
        self.publish_stores("base", {
            1: {"code": "A"}, 2: {"code": "B"}, 3: {"code": "C"},
        })
        self.publish_stores("left", {
            1: {"code": "X"}, 2: {"code": "B"}, 3: {"code": "C"},
        })
        self.publish_stores("right", {
            1: {"code": "A"}, 2: {"code": "X"}, 3: {"code": "C"},
        })

    def cli(self, *args):
        return subprocess.run(
            [sys.executable, "-I", ENTRY, *args],
            cwd=PROJECT, capture_output=True, text=True, check=False,
        )

    def reconcile_cli(self, output_id, decisions=None):
        args = [
            "reconcile-table",
            "--registry", self.registry.root,
            "--base", "base", "--left", "left", "--right", "right",
            "--table", "stores", "--output-id", output_id,
        ]
        if decisions is not None:
            args += ["--decisions", decisions]
        return self.cli(*args)

    def test_view_then_submit_round_trip(self):
        proc = self.reconcile_cli("merged")
        self.assertEqual(proc.returncode, 3, msg=proc.stderr)
        lines = [json.loads(x) for x in proc.stdout.splitlines()]
        self.assertEqual(lines[0]["stage"], "keys")
        token = lines[0]["token"]
        self.assertEqual([(r["key"], r["row_id"]) for r in lines[1:]],
                         [("X", 1), ("X", 2)])

        decisions = self.write_decisions(
            [
                {"kind": "decision", "conflict": "key", "row_id": 1,
                 "key": "B"},
                {"kind": "decision", "conflict": "key", "row_id": 2,
                 "key": "X"},
            ],
            token=token,
        )
        proc = self.reconcile_cli("merged", decisions)
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["outcome"], "published")
        self.assertEqual(payload["output_id"], "merged")
        self.assertEqual(payload["key_conflicts"], 1)
        rows = self.stores_rows(self.registry.open("merged"))
        self.assertEqual(
            {rid: row["code"] for rid, row in rows.items()},
            {1: "B", 2: "X", 3: "C"},
        )
        self.assertEqual(self.registry.current_id(), "right")

        # Same request again: idempotent success.
        proc = self.reconcile_cli("merged", decisions)
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["outcome"], "unchanged")

    def test_cli_distinct_exit_codes(self):
        # Invalid decisions -> 2
        bad = self.write_decisions([
            {"kind": "decision", "conflict": "key", "row_id": 1,
             "key": "B"},
        ])  # key rulings need the token
        proc = self.reconcile_cli("merged", bad)
        self.assertEqual(proc.returncode, 2)
        self.assertIn("token", proc.stderr)
        self.assertFalse(os.path.exists(
            os.path.join(self.registry.snapshots_dir, "merged")))

        # Publish once, then reuse the id for different content -> 4
        header = self.pending("merged", stage="keys")[0]
        good = self.write_decisions(
            [
                {"kind": "decision", "conflict": "key", "row_id": 1,
                 "key": "B"},
                {"kind": "decision", "conflict": "key", "row_id": 2,
                 "key": "X"},
            ],
            token=header["token"], name="good.jsonl",
        )
        self.assertEqual(self.reconcile_cli("merged", good).returncode, 0)
        other = self.write_decisions(
            [
                {"kind": "decision", "conflict": "key", "row_id": 1,
                 "key": "Q"},
                {"kind": "decision", "conflict": "key", "row_id": 2,
                 "key": "X"},
            ],
            token=header["token"], name="other.jsonl",
        )
        proc = self.reconcile_cli("merged", other)
        self.assertEqual(proc.returncode, 4)
        rows = self.stores_rows(self.registry.open("merged"))
        self.assertEqual(rows[1]["code"], "B")

        # Unknown snapshot -> 2
        proc = self.cli(
            "reconcile-table", "--registry", self.registry.root,
            "--base", "nope", "--left", "left", "--right", "right",
            "--table", "stores", "--output-id", "x",
        )
        self.assertEqual(proc.returncode, 2)


if __name__ == "__main__":
    unittest.main()
