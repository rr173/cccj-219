"""Reader/snapshot tests: the five distinct observables and delivery order."""

import os
import tempfile
import unittest

from bcache import MISSING, Reader, SnapshotBuilder, eq
from bcache.errors import KeyNotFound, TableNotFound, UnknownColumn
from bcache.snapshot import SnapshotRegistry


class ReaderObservableTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.registry = SnapshotRegistry(os.path.join(self.tmp, "reg"))

    def _publish(self, name, build_fn):
        path = os.path.join(self.registry.snapshots_dir, name)
        snapshot = build_fn(SnapshotBuilder(path))
        self.registry.publish(snapshot)
        return self.registry.pin(snapshot.snapshot_id)

    def test_missing_table_vs_empty_table_vs_missing_key(self):
        def build(builder):
            builder.add_table("empty", [("k", "text"), ("v", "int64")], key_col="k")
            builder.add_table("keys", [("k", "text"), ("v", "int64")], key_col="k")
            builder.add_row("keys", 1, {"k": "present", "v": 7})
            return builder.finalize()

        snap = self._publish("s1", build)
        reader = Reader(snap, snap.connect())
        try:
            # 1. table does not exist
            self.assertFalse(reader.exists("ghost"))
            ghost = reader.lookup("ghost", "x")
            self.assertEqual(ghost.status, "table_not_found")
            with self.assertRaises(TableNotFound):
                list(reader.read("ghost", None, ["v"]))

            # 2. empty table is a different observation from missing table
            self.assertTrue(reader.exists("empty"))
            empty = reader.lookup("empty", "x")
            self.assertEqual(empty.status, "empty_table")
            self.assertEqual(list(reader.read("empty", None, ["v"])), [])

            # 3. existing table, declared key, but key absent
            miss = reader.lookup("keys", "absent", key_col="k")
            self.assertEqual(miss.status, "key_not_found")
            hit = reader.lookup("keys", "present", key_col="k")
            self.assertEqual(hit.status, "found")
            self.assertEqual(hit.value["v"], 7)
        finally:
            reader.conn.close()

    def test_unknown_column_distinct_from_null_cell(self):
        def build(builder):
            builder.add_table("t", [("a", "int64"), ("b", "int64")])
            builder.add_row("t", 1, {"a": 1, "b": None})  # explicit NULL
            builder.add_row("t", 2, {"a": 2})             # missing b cell
            return builder.finalize()

        snap = self._publish("s2", build)
        reader = Reader(snap, snap.connect())
        try:
            with self.assertRaises(UnknownColumn):
                list(reader.read("t", None, ["nope"]))
            rows = list(reader.read("t", None, ["a", "b"]))
            self.assertEqual(rows[0].values["b"], None)
            self.assertIs(rows[1].values["b"], MISSING)
            self.assertNotEqual(rows[0].values["b"], MISSING)
        finally:
            reader.conn.close()

    def test_read_stable_row_id_order_and_predicate(self):
        def build(builder):
            builder.add_table("t", [("region", "text"), ("amount", "int64")])
            # Insert out of order; delivery must still follow row_id.
            builder.add_row("t", 3, {"region": "N", "amount": 30})
            builder.add_row("t", 1, {"region": "N", "amount": 10})
            builder.add_row("t", 2, {"region": "S", "amount": 20})
            return builder.finalize()

        snap = self._publish("s3", build)
        reader = Reader(snap, snap.connect())
        try:
            rows = list(reader.read("t", eq("region", "N"), ["amount"]))
            self.assertEqual([r.row_id for r in rows], [1, 3])
            self.assertEqual([r.values["amount"] for r in rows], [10, 30])
        finally:
            reader.conn.close()

    def test_list_partitions_prefix_semantics(self):
        def build(builder):
            builder.add_table("daily/2026-01-01", [("v", "int64")])
            builder.add_table("daily/2026-01-02", [("v", "int64")])
            builder.add_table("weekly/w1", [("v", "int64")])
            return builder.finalize()

        snap = self._publish("s4", build)
        reader = Reader(snap, snap.connect())
        try:
            self.assertEqual(
                reader.list_partitions("daily/"),
                ["daily/2026-01-01", "daily/2026-01-02"],
            )
            self.assertEqual(reader.list_partitions("missing/"), [])
        finally:
            reader.conn.close()


if __name__ == "__main__":
    unittest.main()
