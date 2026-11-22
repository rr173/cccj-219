"""Independent acceptance tests for the two hand-written contract jobs.

For every scenario we:

* derive the new snapshot from the baseline snapshot directory,
* build the hand-written observation checklist on both snapshots,
* derive the expected decision ourselves (``tests/direct.py``), without ever
  calling the product's replayer or invalidation logic,
* run the product, assert its mode matches the independent verdict, and
* cross-check every output against a cache-disabled direct job execution.
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bcache import CacheStore, SnapshotBuilder, SnapshotRegistry  # noqa: E402

from tests import direct  # noqa: E402
from tests.fixtures import (  # noqa: E402
    build_partition_snapshot,
    build_sales_snapshot,
)

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RATE_JOB = os.path.join(PROJECT, "jobs", "rate_sales.py")
PART_JOB = os.path.join(PROJECT, "jobs", "daily_parts.py")


class AcceptanceBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.registry = SnapshotRegistry(os.path.join(self.tmp, "registry"))
        self.cache_dir = os.path.join(self.tmp, "cache")
        self.store = CacheStore(self.cache_dir)
        self._snap_counter = 0
        self._uid = 0

    def tearDown(self):
        self.store.close()

    def _next_id(self) -> str:
        self._uid += 1
        return "snap-%d-%d" % (id(self) % 100000, self._uid)

    def _staging(self, label: str) -> str:
        self._uid += 1
        path = os.path.join(self.tmp, "staging", "%s-%d" % (label, self._uid))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def publish(self, snapshot):
        # Snapshot ids are externally supplied unique names; digests still
        # authenticate content. Tests must never rely on digest-prefix ids.
        self.registry.publish(snapshot)
        return snapshot

    def derive(self, base, fn):
        self._snap_counter += 1
        builder = SnapshotBuilder(
            self._staging("derived-%d" % self._snap_counter), derive_from=base
        )
        fn(builder)
        snap = builder.finalize(snapshot_id=self._next_id())
        self.registry.publish(snap)
        return snap

    def product_run(self, job, snapshot):
        return self.store.run(
            job, self.registry, snapshot_id=snapshot.snapshot_id
        )

    def assert_decision(self, job, base, new, checklist_fn):
        base_list = checklist_fn(base)
        new_list = checklist_fn(new)
        expected = direct.expected_decision(base_list, new_list)
        result = self.product_run(job, new)
        self.assertEqual(
            result.mode, expected,
            msg="product said %s, hand-written checklist says %s; "
            "base=%r new=%r" % (result.mode, expected, base_list, new_list),
        )
        baseline = direct.direct_run(job, new)
        self.assertEqual(
            [dict(r) for r in result.output.rows], baseline["rows"]
        )
        self.assertEqual(result.output.digest, baseline["digest"])
        return result, expected


class RateSalesAcceptance(AcceptanceBase):
    def baseline(self):
        snap = build_sales_snapshot(
            self._staging("rate-base"), [2, 3], s_amounts=[99]
        )
        return self.publish(snap)

    def test_initial_run_is_50_with_one_launch(self):
        base = self.baseline()
        result = self.product_run(RATE_JOB, base)
        self.assertEqual(result.mode, "executed")
        self.assertEqual(result.launches, 1)
        self.assertEqual([dict(r) for r in result.output.rows], [{"total": 50}])
        # Cache-disabled business baseline agrees.
        self.assertEqual(direct.direct_run(RATE_JOB, base)["rows"], [{"total": 50}])
        self.assertEqual(self.store.launches_for_job(result.job_version), 1)

    def test_rerun_on_unchanged_snapshot_reuses_zero_launches(self):
        base = self.baseline()
        first = self.product_run(RATE_JOB, base)
        second = self.product_run(RATE_JOB, base)
        self.assertEqual(second.mode, "reused")
        self.assertEqual(second.launches, 0)
        self.assertEqual(second.entry_id, first.entry_id)
        self.assertEqual(self.store.launches_for_job(first.job_version), 1)

    def test_change_to_s_region_reuses_50(self):
        base = self.baseline()
        self.product_run(RATE_JOB, base)
        # S region row is outside the predicate: modify it freely.
        new = self.derive(
            base,
            lambda b: (
                b.update_cell("sales", 3, "amount", 1234),
            ),
        )
        result, expected = self.assert_decision(
            RATE_JOB, base, new, direct.rate_sales_checklist
        )
        self.assertEqual(expected, "reused")
        self.assertEqual([dict(r) for r in result.output.rows], [{"total": 50}])
        self.assertEqual(self.store.launches_for_job(result.job_version), 1)

    def test_change_to_unprojected_remark_reuses_50(self):
        base = self.publish(
            build_sales_snapshot(
                self._staging("remark-base"), [2, 3],
                remarks={1: "old note", 2: "other"},
            )
        )
        self.product_run(RATE_JOB, base)
        new = self.derive(
            base,
            lambda b: b.update_cell("sales", 1, "remark", "changed note"),
        )
        result, expected = self.assert_decision(
            RATE_JOB, base, new, direct.rate_sales_checklist
        )
        self.assertEqual(expected, "reused")
        self.assertEqual([dict(r) for r in result.output.rows], [{"total": 50}])

    def test_add_missing_rate_key_a_reruns_to_20(self):
        base = self.baseline()
        # rates table exists but has no key A.
        with_rates = self.derive(
            base,
            lambda b: b.add_table(
                "rates", [("code", "text"), ("value", "int64")], key_col="code"
            ),
        )
        first = self.product_run(RATE_JOB, with_rates)
        self.assertEqual(first.mode, "executed")
        self.assertEqual([dict(r) for r in first.output.rows], [{"total": 50}])

        filled = self.derive(
            with_rates,
            lambda b: b.add_row("rates", 1, {"code": "A", "value": 4}),
        )
        result, expected = self.assert_decision(
            RATE_JOB, with_rates, filled, direct.rate_sales_checklist
        )
        self.assertEqual(expected, "executed")
        self.assertEqual([dict(r) for r in result.output.rows], [{"total": 20}])
        self.assertEqual(self.store.launches_for_job(first.job_version), 2)

    def test_add_matching_n_row_reruns_to_40_not_just_old_rows(self):
        base = self.publish(
            build_sales_snapshot(
                self._staging("nrow-base"), [2, 3], rates={"A": 4}
            )
        )
        first = self.product_run(RATE_JOB, base)
        self.assertEqual([dict(r) for r in first.output.rows], [{"total": 20}])

        new = self.derive(
            base,
            # row_id 4 is a brand new predicate-matching row.
            lambda b: b.add_row("sales", 4, {"region": "N", "amount": 5}),
        )
        # Ask for the explanation BEFORE any product run on this snapshot:
        # afterwards a fresh entry would make why_recompute legitimately
        # report reuse.
        why = self.store.why_recompute(RATE_JOB, self.registry,
                                       snapshot_id=new.snapshot_id)
        self.assertFalse(why.reusable)
        self.assertIn("extra row", why.reason)
        result, expected = self.assert_decision(
            RATE_JOB, base, new, direct.rate_sales_checklist
        )
        self.assertEqual(expected, "executed")
        self.assertEqual([dict(r) for r in result.output.rows], [{"total": 40}])

    def test_delete_matching_row_reruns(self):
        base = self.baseline()
        self.product_run(RATE_JOB, base)
        new = self.derive(base, lambda b: b.delete_row("sales", 2))
        result, expected = self.assert_decision(
            RATE_JOB, base, new, direct.rate_sales_checklist
        )
        self.assertEqual(expected, "executed")
        self.assertEqual(direct.direct_run(RATE_JOB, new)["rows"],
                         [dict(r) for r in result.output.rows])

    def test_rates_table_appearing_reruns_even_with_same_default_value(self):
        base = self.baseline()
        self.product_run(RATE_JOB, base)
        # Empty rates table: exists() flips, lookup returns empty_table, the
        # numeric default stays 10, output is still 50 — but the read path
        # changed, so recomputation is mandatory.
        new = self.derive(
            base,
            lambda b: b.add_table(
                "rates", [("code", "text"), ("value", "int64")], key_col="code"
            ),
        )
        result, expected = self.assert_decision(
            RATE_JOB, base, new, direct.rate_sales_checklist
        )
        self.assertEqual(expected, "executed")
        self.assertEqual([dict(r) for r in result.output.rows], [{"total": 50}])


class PartitionAcceptance(AcceptanceBase):
    def baseline(self):
        snap = build_partition_snapshot(
            self._staging("part-base"),
            {"2026-01-01": [1, 2], "2026-01-02": [3]},
        )
        return self.publish(snap)

    def test_initial_partition_run(self):
        base = self.baseline()
        result = self.product_run(PART_JOB, base)
        self.assertEqual(result.mode, "executed")
        self.assertEqual(
            [dict(r) for r in result.output.rows],
            [{"total": 6, "partitions": 2}],
        )

    def test_unchanged_partitions_reuse(self):
        base = self.baseline()
        self.product_run(PART_JOB, base)
        result = self.product_run(PART_JOB, base)
        self.assertEqual(result.mode, "reused")

    def test_new_empty_matching_partition_forces_rerun_same_number(self):
        base = self.baseline()
        first = self.product_run(PART_JOB, base)
        new = self.derive(
            base,
            lambda b: b.add_table("daily/2026-01-03", [("amount", "int64")]),
        )
        # Explain before the product run, while the only candidate is the
        # entry built on the old snapshot.
        why = self.store.why_recompute(PART_JOB, self.registry,
                                       snapshot_id=new.snapshot_id)
        self.assertFalse(why.reusable)
        self.assertIn("partition", why.reason)
        result, expected = self.assert_decision(
            PART_JOB, base, new, direct.partition_checklist
        )
        self.assertEqual(expected, "executed")
        # Numeric total is unchanged, yet a real launch happened.
        self.assertEqual(
            [dict(r) for r in result.output.rows],
            [{"total": 6, "partitions": 3}],
        )
        self.assertEqual(self.store.launches_for_job(first.job_version), 2)

    def test_change_inside_partition_reruns(self):
        base = self.baseline()
        self.product_run(PART_JOB, base)
        new = self.derive(
            base,
            lambda b: b.update_cell("daily/2026-01-02", 1, "amount", 30),
        )
        result, expected = self.assert_decision(
            PART_JOB, base, new, direct.partition_checklist
        )
        self.assertEqual(expected, "executed")
        self.assertEqual(
            [dict(r) for r in result.output.rows],
            [{"total": 33, "partitions": 2}],
        )


if __name__ == "__main__":
    unittest.main()
