"""Killed-process acceptance: a SIGKILLed job can never become a cache entry.

The run record must not be marked ok, no package may be published, and the
advisory lock must be released automatically when the process dies so a
later executor can take over.
"""

import multiprocessing
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bcache import (  # noqa: E402
    CacheStore,
    SnapshotBuilder,
    SnapshotRegistry,
    build_identity,
)

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _slow_worker(reg_root, cache_root, snap_id, job_path, queue):
    sys.path.insert(0, PROJECT)
    from bcache import CacheStore, SnapshotRegistry

    store = CacheStore(cache_root)
    queue.put("started")
    store.run(job_path, SnapshotRegistry(reg_root), snapshot_id=snap_id)


class KilledJobTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.registry = SnapshotRegistry(os.path.join(self.tmp, "registry"))
        self.cache_dir = os.path.join(self.tmp, "cache")
        b = SnapshotBuilder(os.path.join(self.tmp, "snap"))
        b.add_table("t", [("v", "int64")])
        b.add_row("t", 1, {"v": 1})
        self.snap = b.finalize()
        self.registry.publish(self.snap)
        self.slow_job = os.path.join(self.tmp, "slow.py")
        with open(self.slow_job, "w", encoding="utf-8") as fh:
            fh.write(
                "import time\n"
                "def run(reader, sink, params):\n"
                "    list(reader.read('t', None, ['v']))\n"
                "    time.sleep(30)\n"
                "    sink.declare([('n','int64')])\n"
                "    sink.write_row({'n': 1})\n"
            )
        self.fast_job = os.path.join(self.tmp, "fast.py")
        with open(self.fast_job, "w", encoding="utf-8") as fh:
            fh.write(
                "def run(reader, sink, params):\n"
                "    list(reader.read('t', None, ['v']))\n"
                "    sink.declare([('n','int64')])\n"
                "    sink.write_row({'n': 1})\n"
            )

    def test_killed_job_leaves_no_cache_and_releases_lock(self):
        ctx = multiprocessing.get_context("fork")
        queue = ctx.Queue()
        proc = ctx.Process(
            target=_slow_worker,
            args=(self.registry.root, self.cache_dir,
                  self.snap.snapshot_id, self.slow_job, queue),
        )
        proc.start()
        self.assertEqual(queue.get(timeout=30), "started")
        time.sleep(1)
        proc.kill()
        proc.join()
        self.assertEqual(proc.exitcode, -9)

        store = CacheStore(self.cache_dir)
        try:
            identity = build_identity(self.slow_job)
            # No cache entry, no package, and the run is not falsely "ok".
            self.assertEqual(store.entries_for_job(identity.version), [])
            self.assertEqual(
                [f for f in os.listdir(store.packages_dir)
                 if f.endswith(".bpkg")],
                [],
            )
            for record in store.list_runs(identity.version):
                self.assertNotEqual(record["status"], "ok")

            # The flock died with the process: a different job can acquire it
            # and execute normally.
            result = store.run(
                self.fast_job, self.registry,
                snapshot_id=self.snap.snapshot_id,
            )
            self.assertEqual(result.mode, "executed")
            self.assertEqual(result.launches, 1)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
