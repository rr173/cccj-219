"""Partial (truncated) read acceptance.

A job that stops consuming a read iterator records exactly the prefix it
saw. Rows appended beyond that prefix must not invalidate the cache, while a
change inside the consumed prefix still must.
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bcache import CacheStore, SnapshotBuilder, SnapshotRegistry  # noqa: E402

from tests import direct  # noqa: E402

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOB = os.path.join(PROJECT, "tests", "testjobs", "partial_read.py")


class PartialReadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.registry = SnapshotRegistry(os.path.join(self.tmp, "registry"))
        self.store = CacheStore(os.path.join(self.tmp, "cache"))
        self._n = 0

    def tearDown(self):
        self.store.close()

    def _base(self):
        self._n += 1
        b = SnapshotBuilder(os.path.join(self.tmp, "s-%d" % self._n))
        b.add_table("nums", [("v", "int64")])
        for i, v in enumerate([10, 20, 30, 40], start=1):
            b.add_row("nums", i, {"v": v})
        snap = b.finalize()
        self.registry.publish(snap)
        return snap

    def _derive(self, base, fn, snap_id):
        b = SnapshotBuilder(os.path.join(self.tmp, "d-%s" % snap_id),
                            derive_from=base)
        fn(b)
        snap = b.finalize(snapshot_id=snap_id)
        self.registry.publish(snap)
        return snap

    def test_truncated_evidence_records_only_consumed_prefix(self):
        base = self._base()
        first = self.store.run(JOB, self.registry, params={"limit": 2},
                               snapshot_id=base.snapshot_id)
        self.assertEqual([dict(r) for r in first.output.rows],
                         [{"first": 10, "consumed": 2}])
        detail = self.store.inspect(first.entry_id)
        read_call = [c for c in detail["calls"] if c["method"] == "read"][0]
        parsed = json.loads(read_call["detail_json"])
        self.assertTrue(parsed["truncated"])
        self.assertEqual(parsed["row_count"], 2)
        self.assertEqual(detail["read_row_total"], 2)

    def test_rows_beyond_consumed_prefix_do_not_invalidate(self):
        base = self._base()
        first = self.store.run(JOB, self.registry, params={"limit": 2},
                               snapshot_id=base.snapshot_id)
        # Append a new row after the consumed prefix; also mutate row 4,
        # which the job never reached.
        new = self._derive(
            base,
            lambda b: (
                b.update_cell("nums", 4, "v", 999),
                b.add_row("nums", 5, {"v": 50}),
            ),
            "tail-changed",
        )
        why = self.store.why_recompute(JOB, self.registry,
                                       params={"limit": 2},
                                       snapshot_id=new.snapshot_id)
        self.assertTrue(why.reusable, msg=why.reason)
        result = self.store.run(JOB, self.registry, params={"limit": 2},
                                snapshot_id=new.snapshot_id)
        self.assertEqual(result.mode, "reused")
        self.assertEqual(self.store.launches_for_job(first.job_version), 1)

    def test_change_inside_consumed_prefix_invalidates(self):
        base = self._base()
        first = self.store.run(JOB, self.registry, params={"limit": 2},
                               snapshot_id=base.snapshot_id)
        new = self._derive(
            base, lambda b: b.update_cell("nums", 1, "v", 11), "head-changed"
        )
        why = self.store.why_recompute(JOB, self.registry,
                                       params={"limit": 2},
                                       snapshot_id=new.snapshot_id)
        self.assertFalse(why.reusable)
        result = self.store.run(JOB, self.registry, params={"limit": 2},
                                snapshot_id=new.snapshot_id)
        self.assertEqual(result.mode, "executed")
        self.assertEqual(direct.direct_run(JOB, new, params={"limit": 2})["rows"],
                         [dict(r) for r in result.output.rows])


if __name__ == "__main__":
    unittest.main()
