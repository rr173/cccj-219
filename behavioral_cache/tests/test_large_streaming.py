"""Streaming validation and peak-memory acceptance on large snapshots.

The default size keeps the ordinary suite fast; set
``BCACHE_LARGE_ROWS=1000000`` to exercise the full million-row case.

The test records:

* real job launch count (the cache must add zero launches on reuse),
* how many read rows the replayer actually compared,
* Python-level peak memory (``tracemalloc``) of the replay phase, which must
  stay bounded and far below the size of materialising every row.
"""

import os
import sys
import tempfile
import tracemalloc
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bcache import CacheStore, SnapshotBuilder, SnapshotRegistry  # noqa: E402

from tests import direct  # noqa: E402
from tests.generate_large import build_large_snapshot  # noqa: E402

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RATE_JOB = os.path.join(PROJECT, "jobs", "rate_sales.py")

ROWS = int(os.environ.get("BCACHE_LARGE_ROWS", "50000"))
# A materialised row (dict + projection objects) is comfortably above this;
# if validation ever collected rows, peak memory would scale linearly and
# blow past this bound even at moderate sizes.
PEAK_MEM_BUDGET_BYTES = 25 * 1024 * 1024


class LargeSnapshotStreamingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="bcache-large-")
        cls.registry = SnapshotRegistry(os.path.join(cls.tmp, "registry"))
        cls.cache_dir = os.path.join(cls.tmp, "cache")
        cls.snapshot = build_large_snapshot(
            os.path.join(cls.tmp, "staging"), ROWS
        )
        cls.registry.publish(cls.snapshot)

    def test_01_real_run_then_streaming_reuse_within_memory_budget(self):
        store = CacheStore(self.cache_dir)
        try:
            first = store.run(RATE_JOB, self.registry,
                              snapshot_id=self.snapshot.snapshot_id)
            self.assertEqual(first.mode, "executed")
            self.assertEqual(first.launches, 1)
            expected_rows = direct.direct_run(RATE_JOB, self.snapshot)["rows"]
            self.assertEqual([dict(r) for r in first.output.rows], expected_rows)

            # Replay validation with peak memory measured.
            tracemalloc.start()
            why = store.why_recompute(
                RATE_JOB, self.registry,
                snapshot_id=self.snapshot.snapshot_id,
            )
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            self.assertTrue(why.reusable)
            self.assertGreater(why.replay.read_rows_compared, 0)
            # Every *delivered* (predicate-matching) row must have been
            # compared; non-matching rows are never part of the observation.
            n_matching = sum(1 for rid in range(1, ROWS + 1) if rid % 2 == 0)
            self.assertEqual(why.replay.read_rows_compared, n_matching)

            print(
                "\n[large] rows=%d compared=%d replay_peak_mem=%.2f MiB "
                "budget=%.2f MiB launches=%d"
                % (
                    ROWS,
                    why.replay.read_rows_compared,
                    peak / 1024 / 1024,
                    PEAK_MEM_BUDGET_BYTES / 1024 / 1024,
                    1,
                )
            )
            self.assertLess(peak, PEAK_MEM_BUDGET_BYTES)

            second = store.run(RATE_JOB, self.registry,
                               snapshot_id=self.snapshot.snapshot_id)
            self.assertEqual(second.mode, "reused")
            self.assertEqual(second.launches, 0)
            self.assertEqual(store.launches_for_job(first.job_version), 1)
        finally:
            store.close()

    def test_02_unprojected_column_change_still_reuses_on_large_snapshot(self):
        builder = SnapshotBuilder(
            os.path.join(self.tmp, "staging-note"), derive_from=self.snapshot
        )
        # Only the unprojected "note" column changes; amount/region untouched.
        builder.update_cell("sales", 1, "note", "changed-only-note")
        changed = builder.finalize(snapshot_id="large-note-changed")
        self.registry.publish(changed)

        store = CacheStore(self.cache_dir)
        try:
            why = store.why_recompute(
                RATE_JOB, self.registry, snapshot_id=changed.snapshot_id
            )
            self.assertTrue(why.reusable, msg=why.reason)
            result = store.run(RATE_JOB, self.registry,
                               snapshot_id=changed.snapshot_id)
            self.assertEqual(result.mode, "reused")
            self.assertEqual(store.launches_for_job(result.job_version), 1)
        finally:
            store.close()

    def test_03_one_matching_row_change_invalidates_without_full_collect(self):
        builder = SnapshotBuilder(
            os.path.join(self.tmp, "staging-amount"), derive_from=self.snapshot
        )
        # row_id 2 is region N (even ids are N) and is projected.
        builder.update_cell("sales", 2, "amount", 999999)
        changed = builder.finalize(snapshot_id="large-amount-changed")
        self.registry.publish(changed)

        store = CacheStore(self.cache_dir)
        try:
            why = store.why_recompute(
                RATE_JOB, self.registry, snapshot_id=changed.snapshot_id
            )
            self.assertFalse(why.reusable)
            # It must stop at the first mismatch, not scan everything.
            self.assertIsNotNone(why.replay.mismatched_seq)
            result = store.run(RATE_JOB, self.registry,
                               snapshot_id=changed.snapshot_id)
            self.assertEqual(result.mode, "executed")
            self.assertEqual(result.launches, 1)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
