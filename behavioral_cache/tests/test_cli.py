"""End-to-end CLI acceptance through real subprocesses under ``python3 -I``."""

import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bcache import SnapshotBuilder, SnapshotRegistry  # noqa: E402

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENTRY = os.path.join(PROJECT, "bcache_run.py")
JOB = os.path.join(PROJECT, "jobs", "rate_sales.py")


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.registry = SnapshotRegistry(os.path.join(self.tmp, "registry"))
        self.cache = os.path.join(self.tmp, "cache")
        b = SnapshotBuilder(os.path.join(self.tmp, "snap"))
        b.add_table("sales", [("region", "text"), ("amount", "int64")])
        b.add_row("sales", 1, {"region": "N", "amount": 2})
        b.add_row("sales", 2, {"region": "N", "amount": 3})
        self.snap = b.finalize()
        self.registry.publish(self.snap)

    def cli(self, *args):
        proc = subprocess.run(
            [sys.executable, "-I", ENTRY, *args],
            cwd=PROJECT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        return json.loads(proc.stdout)

    def test_all_four_entries(self):
        run1 = self.cli(
            "run", JOB,
            "--registry", self.registry.root,
            "--cache", self.cache,
            "--snapshot-id", self.snap.snapshot_id,
        )
        self.assertEqual(run1["mode"], "executed")
        self.assertEqual(run1["rows"], [{"total": 50}])
        entry_id = run1["entry_id"]

        run2 = self.cli(
            "run", JOB,
            "--registry", self.registry.root,
            "--cache", self.cache,
            "--snapshot-id", self.snap.snapshot_id,
        )
        self.assertEqual(run2["mode"], "reused")
        self.assertEqual(run2["launches"], 0)

        why = self.cli(
            "why-recompute", JOB,
            "--registry", self.registry.root,
            "--cache", self.cache,
            "--snapshot-id", self.snap.snapshot_id,
        )
        self.assertTrue(why["reusable"])

        inspect = self.cli("inspect", entry_id, "--cache", self.cache)
        methods = [c["method"] for c in inspect["calls"]]
        self.assertEqual(methods, ["exists", "read"])

        verify = self.cli(
            "verify-result", entry_id, "--cache", self.cache
        )
        self.assertTrue(verify["verified"])
        self.assertTrue(verify["package_digest_ok"])
        self.assertTrue(verify["evidence_digest_ok"])


if __name__ == "__main__":
    unittest.main()
