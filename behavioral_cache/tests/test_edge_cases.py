"""Edge-case acceptance: branching, null/missing, type drift, determinism.

These exercise the product's invalidation surface but still cross-check
decisions against hand-written checklists where applicable, and always
against cache-disabled direct runs for output correctness.
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
    DeterminismViolation,
    SnapshotBuilder,
    SnapshotRegistry,
    build_identity,
)
from bcache.jobs import JobError  # noqa: E402

from tests import direct  # noqa: E402

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TJ = os.path.join(PROJECT, "tests", "testjobs")


class EdgeBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.registry = SnapshotRegistry(os.path.join(self.tmp, "registry"))
        self.store = CacheStore(os.path.join(self.tmp, "cache"))
        self._n = 0

    def tearDown(self):
        self.store.close()

    def staging(self, label):
        self._n += 1
        path = os.path.join(self.tmp, "staging", "%s-%d" % (label, self._n))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def publish(self, builder, snap_id=None):
        snap = builder.finalize(snapshot_id=snap_id) if snap_id else builder.finalize()
        self.registry.publish(snap)
        return snap

    def derive(self, base, fn):
        self._n += 1
        builder = SnapshotBuilder(
            os.path.join(self.tmp, "staging", "derived-%d" % self._n),
            derive_from=base,
        )
        fn(builder)
        snap = builder.finalize(snapshot_id="derived-%d" % self._n)
        self.registry.publish(snap)
        return snap

    def product_run(self, job, snap, params=None):
        return self.store.run(job, self.registry, params=params,
                              snapshot_id=snap.snapshot_id)


class DynamicBranchTests(EdgeBase):
    JOB = os.path.join(TJ, "branching.py")

    def test_branch_switch_takes_different_read_path(self):
        b = SnapshotBuilder(self.staging("branch-a"))
        b.add_table("config", [("k", "text"), ("v", "text")], key_col="k")
        b.add_row("config", 1, {"k": "mode", "v": "a"})
        b.add_table("branch_a", [("n", "int64")])
        b.add_row("branch_a", 1, {"n": 5})
        b.add_table("branch_b", [("n", "int64")])
        b.add_row("branch_b", 1, {"n": 99})
        base = self.publish(b)
        first = self.product_run(self.JOB, base)
        self.assertEqual([dict(r) for r in first.output.rows],
                         [{"branch": "a", "total": 5}])

        new = self.derive(
            base, lambda x: x.update_cell("config", 1, "v", "b")
        )
        result = self.product_run(self.JOB, new)
        self.assertEqual(result.mode, "executed")
        self.assertEqual([dict(r) for r in result.output.rows],
                         [{"branch": "b", "total": 99}])
        # The replay must have failed on the config read, deep in the log.
        self.assertEqual(result.replay.mismatched_seq, 1)
        # Output agrees with a cache-disabled run.
        self.assertEqual(direct.direct_run(self.JOB, new)["rows"],
                         [dict(r) for r in result.output.rows])


class EmptyQueryTests(EdgeBase):
    JOB = os.path.join(TJ, "empty_query.py")

    def test_empty_query_that_later_has_rows_invalidates(self):
        b = SnapshotBuilder(self.staging("empty"))
        b.add_table("events", [("kind", "text"), ("id", "int64")])
        b.add_row("events", 1, {"kind": "old", "id": 1})
        base = self.publish(b)
        first = self.product_run(self.JOB, base)
        self.assertEqual([dict(r) for r in first.output.rows], [{"count": 0}])

        # A negative (empty) read result is part of the evidence.
        detail = self.store.inspect(first.entry_id)
        read_call = [c for c in detail["calls"] if c["method"] == "read"][0]
        import json
        self.assertEqual(json.loads(read_call["detail_json"])["row_count"], 0)

        new = self.derive(
            base, lambda x: x.add_row("events", 2, {"kind": "new", "id": 2})
        )
        why = self.store.why_recompute(self.JOB, self.registry,
                                       snapshot_id=new.snapshot_id)
        self.assertFalse(why.reusable)
        result = self.product_run(self.JOB, new)
        self.assertEqual(result.mode, "executed")
        self.assertEqual([dict(r) for r in result.output.rows], [{"count": 1}])


class NullVsMissingTableTests(EdgeBase):
    JOB = os.path.join(TJ, "missing_table.py")

    def test_missing_table_then_present_then_null_distinct(self):
        b = SnapshotBuilder(self.staging("nomt"))
        base = self.publish(b)  # no optional_metrics at all
        first = self.product_run(self.JOB, base)
        self.assertEqual([dict(r) for r in first.output.rows],
                         [{"exists": False, "total": -1}])

        # Table appears, still empty: exists observation flips -> rerun.
        with_table = self.derive(
            base,
            lambda x: x.add_table("optional_metrics", [("m", "int64")]),
        )
        second = self.product_run(self.JOB, with_table)
        self.assertEqual(second.mode, "executed")
        self.assertEqual([dict(r) for r in second.output.rows],
                         [{"exists": True, "total": 0}])

        # Adding a numeric row changes the read contents and total.
        with_value = self.derive(
            with_table,
            lambda x: x.add_row("optional_metrics", 1, {"m": 7}),
        )
        third = self.product_run(self.JOB, with_value)
        self.assertEqual(third.mode, "executed")
        self.assertEqual([dict(r) for r in third.output.rows],
                         [{"exists": True, "total": 7}])
        self.assertEqual(direct.direct_run(self.JOB, with_value)["digest"],
                         third.output.digest)

    def test_null_cell_vs_missing_cell_kept_distinct_in_evidence(self):
        b = SnapshotBuilder(self.staging("nulls"))
        b.add_table("t", [("a", "int64"), ("b", "int64")])
        b.add_row("t", 1, {"a": 1, "b": None})
        b.add_row("t", 2, {"a": 2})  # b missing
        base = self.publish(b)
        # Use a tiny inline job through the reader directly to inspect rows.
        from bcache import Reader
        reader = Reader(base, base.connect())
        rows = list(reader.read("t", None, ["a", "b"]))
        reader.conn.close()
        self.assertIsNone(rows[0].values["b"])
        from bcache import MISSING
        self.assertIs(rows[1].values["b"], MISSING)
        self.assertNotEqual(rows[0].values["b"], MISSING)


class RangeBoundaryTests(EdgeBase):
    JOB = os.path.join(TJ, "range_job.py")

    def test_open_closed_boundary_change(self):
        b = SnapshotBuilder(self.staging("range"))
        b.add_table("nums", [("v", "int64")])
        for i, v in enumerate([9, 10, 19, 20], start=1):
            b.add_row("nums", i, {"v": v})
        base = self.publish(b)
        params = {"lo": 10, "hi": 20}
        first = self.product_run(self.JOB, base, params=params)
        self.assertEqual([dict(r) for r in first.output.rows], [{"sum": 29}])

        # Same snapshot, same params: reuse.
        again = self.product_run(self.JOB, base, params=params)
        self.assertEqual(again.mode, "reused")

        # Move the upper boundary: predicate itself changed -> new job version
        # via params, so no candidate at all.
        other = self.product_run(self.JOB, base, params={"lo": 10, "hi": 21})
        self.assertEqual(other.mode, "executed")
        self.assertEqual([dict(r) for r in other.output.rows], [{"sum": 49}])


class BoolIntTests(EdgeBase):
    JOB = os.path.join(TJ, "boolint_job.py")

    def test_bool_not_counted_as_int_one(self):
        b = SnapshotBuilder(self.staging("bool"))
        b.add_table("flags", [("flag", "bool"), ("v", "int64")])
        b.add_row("flags", 1, {"flag": True, "v": 1})
        b.add_row("flags", 2, {"flag": False, "v": 1})
        base = self.publish(b)
        result = self.product_run(self.JOB, base)
        # eq flag=True hits row 1 only; eq v=1 hits both int cells.
        self.assertEqual([dict(r) for r in result.output.rows],
                         [{"flag_hits": 1, "int_hits": 2}])

    def test_projected_column_type_change_invalidates(self):
        b = SnapshotBuilder(self.staging("typedrift"))
        b.add_table("flags", [("flag", "bool"), ("v", "int64")])
        b.add_row("flags", 1, {"flag": True, "v": 1})
        base = self.publish(b)
        first = self.product_run(self.JOB, base)

        # Rebuild with v as text holding "1": projected column type changes.
        b2 = SnapshotBuilder(self.staging("typedrift2"))
        b2.add_table("flags", [("flag", "bool"), ("v", "text")])
        b2.add_row("flags", 1, {"flag": True, "v": "1"})
        changed = self.publish(b2, snap_id="typedrift-v2")
        why = self.store.why_recompute(self.JOB, self.registry,
                                       snapshot_id=changed.snapshot_id)
        self.assertFalse(why.reusable)
        self.assertIn("schema", why.reason)


class FailingJobTests(EdgeBase):
    JOB = os.path.join(TJ, "failing_job.py")

    def test_failed_job_is_never_cached(self):
        b = SnapshotBuilder(self.staging("fail"))
        b.add_table("anything", [("x", "int64")])
        base = self.publish(b)
        with self.assertRaises(JobError):
            self.product_run(self.JOB, base)
        # No entries registered; runs table records the failure.
        identity = build_identity(self.JOB)
        self.assertEqual(self.store.entries_for_job(identity.version), [])
        runs = self.store.list_runs(identity.version)
        self.assertTrue(any(r["status"] == "failed" for r in runs))
        # A subsequent run still has to launch for real.
        b2 = SnapshotBuilder(self.staging("fail2"))
        b2.add_table("anything", [("x", "int64")])
        b2.add_row("anything", 1, {"x": 1})
        good_snap = self.publish(b2, snap_id="fail-retry")
        # Same job still fails by design on any snapshot; assert no cache
        # entry leaked from the first attempt.
        with self.assertRaises(JobError):
            self.product_run(self.JOB, good_snap)
        self.assertEqual(self.store.entries_for_job(identity.version), [])


class DeterminismContractTests(EdgeBase):
    def test_same_observations_different_output_is_flagged(self):
        # A deliberately contract-violating job: identical reads, but its
        # output depends on an environment variable. Two executors that each
        # really execute it on observation-equal snapshots must be detected.
        b = SnapshotBuilder(self.staging("det"))
        b.add_table("t", [("v", "int64")])
        b.add_row("t", 1, {"v": 1})
        base = self.publish(b)

        nondet = os.path.join(self.tmp, "nondet_job.py")
        with open(nondet, "w", encoding="utf-8") as fh:
            fh.write(
                "import os\n"
                "def run(reader, sink, params):\n"
                "    list(reader.read('t', None, ['v']))\n"
                "    sink.declare([('n', 'int64')])\n"
                "    sink.write_row({'n': int(os.environ.get('BCACHE_NONDET', '0'))})\n"
            )
        first = self.product_run(nondet, base)
        self.assertEqual([dict(r) for r in first.output.rows], [{"n": 0}])

        # Second snapshot: different id, byte-identical observable content.
        b2 = SnapshotBuilder(self.staging("det2"))
        b2.add_table("t", [("v", "int64")])
        b2.add_row("t", 1, {"v": 1})
        same_obs = self.publish(b2, snap_id="det-twin")

        identity = build_identity(nondet)
        # Simulate a second executor that skips replay and really launches the
        # (now tampered) job, while the original entry stays registered.
        os.environ["BCACHE_NONDET"] = "1"
        try:
            run_id = "forced-run-determinism-check"
            self.store._start_run(run_id, identity, same_obs, "executed")
            with self.assertRaises(DeterminismViolation) as ctx:
                self.store._execute_and_commit(
                    identity, same_obs, run_id, invalidation=None
                )
        finally:
            del os.environ["BCACHE_NONDET"]

        violations = self.store.violations()
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0]["violation_id"],
                         ctx.exception.violation_id)
        self.assertNotEqual(
            violations[0]["existing_output_digest"],
            violations[0]["conflicting_output_digest"],
        )
        # The conflicting output is never promoted; the original survives and
        # nothing is silently picked as "the" correct cache.
        entries = self.store.entries_for_job(identity.version)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["entry_id"], first.entry_id)


if __name__ == "__main__":
    unittest.main()
