"""Concurrency acceptance: execution ownership and snapshot generation pinning.

Runs real OS processes (``multiprocessing``) against one shared cache store
and registry:

* competing executors for one logical request serialize through the advisory
  lock: exactly one real job launch, every other process reuses the result;
* one process publishes first and a second later verifies and reuses;
* a snapshot published *during* another process's validation/run cannot mix
  generations into its reads, because each operation pins its snapshot;
* no half-written package ever becomes visible.
"""

import multiprocessing
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bcache import CacheStore, SnapshotBuilder, SnapshotRegistry  # noqa: E402

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RATE_JOB = os.path.join(PROJECT, "jobs", "rate_sales.py")


def _worker_run(args):
    registry_root, cache_root, snapshot_id, job, delay = args
    # Top-level worker function: spawn uses fork on Linux, but stay explicit.
    sys.path.insert(0, PROJECT)
    from bcache import CacheStore, SnapshotRegistry

    registry = SnapshotRegistry(registry_root)
    store = CacheStore(cache_root)
    try:
        if delay:
            time.sleep(delay)
        result = store.run(job, registry, snapshot_id=snapshot_id)
        return {
            "mode": result.mode,
            "entry_id": result.entry_id,
            "launches": result.launches,
            "rows": [dict(r) for r in result.output.rows],
        }
    finally:
        store.close()


def _worker_publish_then_run(args):
    registry_root, cache_root, base_path, snapshot_id, job = args
    sys.path.insert(0, PROJECT)
    from bcache import CacheStore, SnapshotBuilder, SnapshotRegistry

    # This process publishes a NEW snapshot while another is mid-validation.
    time.sleep(0.2)
    builder = SnapshotBuilder(
        os.path.join(registry_root, "staging-concurrent"), derive_from=None
    )
    builder.add_table("sales", [("region", "text"), ("amount", "int64")])
    builder.add_row("sales", 1, {"region": "N", "amount": 1000})
    snap = builder.finalize(snapshot_id=snapshot_id)
    registry = SnapshotRegistry(registry_root)
    registry.publish(snap)

    store = CacheStore(cache_root)
    try:
        result = store.run(job, registry, snapshot_id=snapshot_id)
        return {
            "mode": result.mode,
            "entry_id": result.entry_id,
            "rows": [dict(r) for r in result.output.rows],
        }
    finally:
        store.close()


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.registry_root = os.path.join(self.tmp, "registry")
        self.cache_root = os.path.join(self.tmp, "cache")
        self.registry = SnapshotRegistry(self.registry_root)
        ctx = multiprocessing.get_context("fork")
        self.pool = ctx.Pool(processes=4)

    def tearDown(self):
        self.pool.close()
        self.pool.join()

    def _base_snapshot(self):
        builder = SnapshotBuilder(os.path.join(self.tmp, "staging-base"))
        builder.add_table("sales", [("region", "text"), ("amount", "int64")])
        builder.add_row("sales", 1, {"region": "N", "amount": 2})
        builder.add_row("sales", 2, {"region": "N", "amount": 3})
        snap = builder.finalize()
        self.registry.publish(snap)
        return snap

    def test_competing_executors_single_launch_all_reuse(self):
        snap = self._base_snapshot()
        # Stagger starts so some arrive while the first is still executing.
        jobs = [
            (self.registry_root, self.cache_root, snap.snapshot_id, RATE_JOB,
             0.0 if i == 0 else 0.05 * i)
            for i in range(4)
        ]
        results = self.pool.map(_worker_run, jobs)
        launches = sum(1 for r in results if r["mode"] == "executed")
        self.assertEqual(launches, 1, results)
        self.assertEqual(sum(r["launches"] for r in results), 1)
        # Everyone resolved to the same logical entry and output.
        entry_ids = {r["entry_id"] for r in results}
        self.assertEqual(len(entry_ids), 1)
        for r in results:
            self.assertEqual(r["rows"], [{"total": 50}])

        store = CacheStore(self.cache_root)
        try:
            identity = self._job_version()
            self.assertEqual(store.launches_for_job(identity), 1)
            # Exactly one complete package, no tmp half-packages.
            packages = [
                f for f in os.listdir(store.packages_dir) if f.endswith(".bpkg")
            ]
            self.assertEqual(len(packages), 1)
            leftovers = [
                f for f in os.listdir(store.packages_dir) if f.endswith(".tmp")
            ]
            self.assertEqual(leftovers, [])
        finally:
            store.close()

    def test_publish_first_then_second_verifies_and_reuses(self):
        snap = self._base_snapshot()
        first = self.pool.apply_async(
            _worker_run,
            [(self.registry_root, self.cache_root, snap.snapshot_id,
              RATE_JOB, 0.0)],
        )
        published = first.get()
        self.assertEqual(published["mode"], "executed")

        # A separate process later only verifies and reuses.
        second = self.pool.apply_async(
            _worker_run,
            [(self.registry_root, self.cache_root, snap.snapshot_id,
              RATE_JOB, 0.0)],
        )
        reused = second.get()
        self.assertEqual(reused["mode"], "reused")
        self.assertEqual(reused["entry_id"], published["entry_id"])
        self.assertEqual(reused["launches"], 0)

    def test_concurrent_snapshot_publish_cannot_mix_generations(self):
        snap = self._base_snapshot()
        # Process A validates/reuses against the pinned original snapshot while
        # process B publishes and runs against a brand new one concurrently.
        args_a = (
            self.registry_root, self.cache_root, snap.snapshot_id,
            RATE_JOB, 0.35,
        )
        args_b = (
            self.registry_root, self.cache_root,
            os.path.join(self.registry_root, "staging-concurrent"),
            "snap-concurrent-1", RATE_JOB,
        )
        async_a = self.pool.apply_async(_worker_run, [args_a])
        async_b = self.pool.apply_async(_worker_publish_then_run, [args_b])
        result_a = async_a.get()
        result_b = async_b.get()

        # A stayed on its pinned generation regardless of B's publish.
        self.assertEqual(result_a["rows"], [{"total": 50}])
        # B's snapshot has no rates table: the job falls back to rate 10,
        # so 1000 * 10 = 10000.
        self.assertEqual(result_b["rows"], [{"total": 10000}])
        self.assertNotEqual(result_a["entry_id"], result_b["entry_id"])

        store = CacheStore(self.cache_root)
        try:
            verify_a = store.verify_result(result_a["entry_id"])
            verify_b = store.verify_result(result_b["entry_id"])
            self.assertTrue(verify_a["verified"])
            self.assertTrue(verify_b["verified"])
        finally:
            store.close()

    def _job_version(self):
        from bcache import build_identity

        return build_identity(RATE_JOB).version


if __name__ == "__main__":
    import tempfile

    unittest.main()
