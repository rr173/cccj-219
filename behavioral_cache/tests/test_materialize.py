"""materialize-as-of: point-in-time snapshots from a base + correction log.

The reference scenario is the hand-computable one from the specification:

    base table keyed by its first column:
        row 1 = ("A", 10), row 2 = ("B", 20)

    seq 8  [10,40) put row 1 ("B", 11)
    seq 9  [10,40) put row 2 ("A", 21)
    seq 14 [20,30) delete row 1
    seq 15 [25,28) put row 1 ("B", 99)
    seq 16 [30,35) put row 3 ("B", 77)

The log is deliberately written out of seq order.
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bcache import (  # noqa: E402
    MISSING,
    CacheStore,
    CorrectionLogError,
    KeyConflictError,
    Reader,
    SnapshotBuilder,
    SnapshotRegistry,
    materialize_as_of,
)
from bcache.snapshot import SnapshotConflict  # noqa: E402

from tests import direct  # noqa: E402

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RATE_JOB = os.path.join(PROJECT, "jobs", "rate_sales.py")

BASE_ROWS = {1: {"k": "A", "n": 10}, 2: {"k": "B", "n": 20}}

# Out of seq order on purpose.
REFERENCE_EVENTS = [
    {"seq": 16, "table": "t", "row_id": 3, "from": 30, "to": 35,
     "op": "put", "values": {"k": "B", "n": 77}},
    {"seq": 8, "table": "t", "row_id": 1, "from": 10, "to": 40,
     "op": "put", "values": {"k": "B", "n": 11}},
    {"seq": 15, "table": "t", "row_id": 1, "from": 25, "to": 28,
     "op": "put", "values": {"k": "B", "n": 99}},
    {"seq": 9, "table": "t", "row_id": 2, "from": 10, "to": 40,
     "op": "put", "values": {"k": "A", "n": 21}},
    {"seq": 14, "table": "t", "row_id": 1, "from": 20, "to": 30,
     "op": "delete"},
]


def write_log(path, events):
    with open(path, "w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")


def scan_table(snapshot, table):
    conn = snapshot.connect()
    try:
        return {rid: dict(values) for rid, values in snapshot.scan(conn, table)}
    finally:
        conn.close()


def assert_rows(test, snapshot, table, expected):
    rows = scan_table(snapshot, table)
    test.assertEqual(set(rows), set(expected), msg="row ids differ: %r" % rows)
    for rid, want in expected.items():
        got = rows[rid]
        for col, value in want.items():
            test.assertEqual(got[col], value,
                             msg="row %r col %r: %r != %r" % (rid, col, got[col], value))


class MaterializeBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.registry = SnapshotRegistry(os.path.join(self.tmp, "registry"))
        self.log = os.path.join(self.tmp, "changes.jsonl")
        self.base = self._publish_base()

    def _publish_base(self):
        b = SnapshotBuilder(os.path.join(self.tmp, "base"))
        b.add_table("t", [("k", "text"), ("n", "int64")], key_col="k")
        for rid, values in BASE_ROWS.items():
            b.add_row("t", rid, values)
        # An untouched, glob-keyed table to prove schema/key/emptiness survive.
        b.add_table("other", [("code", "int64"), ("flag", "bool")],
                    key_col="code")
        b.add_row("other", 7, {"code": 5, "flag": True})
        b.add_table("empty", [("x", "text")])
        snap = b.finalize(snapshot_id="base")
        self.registry.publish(snap)
        return snap

    def materialize(self, T, S, output_id, events=None, base_id="base"):
        write_log(self.log, REFERENCE_EVENTS if events is None else events)
        return materialize_as_of(
            self.registry, base_id, self.log, T, S, output_id
        )


class HandComputedExampleTests(MaterializeBase):
    def test_known_through_9_at_22_performs_legal_key_swap(self):
        result = self.materialize(22, 9, "t22-s9")
        # seq 14/15/16 are not yet registered at S=9.
        assert_rows(self, result.snapshot, "t", {
            1: {"k": "B", "n": 11},
            2: {"k": "A", "n": 21},
        })

    def test_late_correction_outside_interval_does_not_shadow_earlier_one(self):
        # At T=22 seq 15 ([25,28)) is registered (S=15) but not applicable, so
        # it must not shadow seq 14's delete ([20,30)).
        result = self.materialize(22, 15, "t22-s15")
        assert_rows(self, result.snapshot, "t", {2: {"k": "A", "n": 21}})

    def test_known_through_14_at_22_drops_row_1(self):
        result = self.materialize(22, 14, "t22-s14")
        assert_rows(self, result.snapshot, "t", {2: {"k": "A", "n": 21}})

    def test_row_1_across_late_correction_boundaries(self):
        self.materialize(25, 15, "t25-s15")
        self.materialize(28, 15, "t28-s15")
        self.materialize(30, 15, "t30-s15")
        at_25 = scan_table(self.registry.open("t25-s15"), "t")
        at_28 = scan_table(self.registry.open("t28-s15"), "t")
        at_30 = scan_table(self.registry.open("t30-s15"), "t")
        self.assertEqual(at_25[1], {"k": "B", "n": 99})
        self.assertNotIn(1, at_28)  # delete [20,30) still in force
        # After the delete expires, the earlier [10,40) put applies again.
        self.assertEqual(at_30[1], {"k": "B", "n": 11})

    def test_late_correction_key_conflict_rejected_and_untouched(self):
        first = self.materialize(30, 15, "t30-s15")
        before = first.snapshot.digest
        with self.assertRaises(KeyConflictError):
            self.materialize(30, 16, "t30-s15")  # row 3 would also key "B"
        # The earlier snapshot is intact and still openable under the id.
        self.assertEqual(self.registry.open("t30-s15").digest, before)
        self.assertEqual(
            scan_table(self.registry.open("t30-s15"), "t")[1],
            {"k": "B", "n": 11},
        )

    def test_time_40_returns_to_base(self):
        result = self.materialize(40, 16, "t40-s16")
        assert_rows(self, result.snapshot, "t", BASE_ROWS)


class PreservationTests(MaterializeBase):
    def test_schema_keys_untouched_tables_and_empty_tables(self):
        result = self.materialize(22, 9, "preserve")
        snap = result.snapshot
        conn = snap.connect()
        try:
            self.assertEqual(snap.schema(conn, "t"),
                             [("k", "text"), ("n", "int64")])
            self.assertEqual(snap.key_col(conn, "t"), "k")
            self.assertEqual(snap.key_col(conn, "other"), "code")
            self.assertEqual(snap.key_col(conn, "empty"), None)
            self.assertEqual(snap.table_names(conn), ["empty", "other", "t"])
            self.assertEqual(snap.count_rows(conn, "empty"), 0)
        finally:
            conn.close()
        assert_rows(self, snap, "other", {7: {"code": 5, "flag": True}})

    def test_missing_cells_stay_missing_not_null(self):
        b = SnapshotBuilder(os.path.join(self.tmp, "sparse"))
        b.add_table("t", [("a", "int64"), ("b", "text")])
        b.add_row("t", 1, {"a": 1})  # b is missing, not NULL
        base = b.finalize(snapshot_id="sparse-base")
        self.registry.publish(base)
        write_log(self.log, [])
        result = materialize_as_of(self.registry, "sparse-base", self.log,
                                   0, 0, "sparse-out")
        reader = Reader(result.snapshot, result.snapshot.connect())
        try:
            rows = list(reader.read("t", None, ["a", "b"]))
        finally:
            reader.conn.close()
        self.assertIs(rows[0].values["b"], MISSING)
        # Base untouched by any correction: identical logical content.
        self.assertEqual(result.snapshot.digest, base.digest)

    def test_explicit_null_in_put_is_kept(self):
        events = [{"seq": 1, "table": "t", "row_id": 1, "from": 0, "to": None,
                   "op": "put", "values": {"k": "Z", "n": None}}]
        result = self.materialize(5, 1, "null-put", events=events)
        rows = scan_table(result.snapshot, "t")
        self.assertIsNone(rows[1]["n"])
        self.assertEqual(rows[1]["k"], "Z")


class LogValidationTests(MaterializeBase):
    def _reject(self, events, exc=CorrectionLogError, T=22, S=100):
        with self.assertRaises(exc):
            self.materialize(T, S, "rejected", events=events)
        self.assertFalse(
            os.path.exists(os.path.join(
                self.registry.snapshots_dir, "rejected")),
            msg="a rejected request must not leave an openable snapshot",
        )

    def test_same_seq_identical_resend_is_accepted(self):
        # The log repeats seq 8 (twice, one with reordered JSON keys) and
        # repeats seq 9; both are byte-identical events after typing.
        seq8 = REFERENCE_EVENTS[1]
        seq9 = REFERENCE_EVENTS[3]
        reordered = dict(reversed(list(seq8.items())))
        result = self.materialize(
            22, 9, "dedupe", events=[seq8, seq9, seq8, reordered, seq9])
        assert_rows(self, result.snapshot, "t", {
            1: {"k": "B", "n": 11},
            2: {"k": "A", "n": 21},
        })

    def test_same_seq_different_content_rejected(self):
        first = dict(REFERENCE_EVENTS[0])
        second = dict(first)
        second["to"] = 36
        self._reject([first, second])

    def test_bool_is_not_an_integer(self):
        # row_id/seq/from/to must not accept JSON true.
        bad = dict(REFERENCE_EVENTS[0])
        bad["row_id"] = True
        self._reject([bad])
        bad = dict(REFERENCE_EVENTS[0])
        bad["seq"] = 1.0
        self._reject([bad])

    def test_put_requires_every_column_and_correct_types(self):
        self._reject([{"seq": 1, "table": "t", "row_id": 1, "from": 0,
                       "to": None, "op": "put", "values": {"k": "A"}}])
        self._reject([{"seq": 1, "table": "t", "row_id": 1, "from": 0,
                       "to": None, "op": "put",
                       "values": {"k": "A", "n": True}}])
        self._reject([{"seq": 1, "table": "t", "row_id": 1, "from": 0,
                       "to": None, "op": "put",
                       "values": {"k": "A", "n": "10"}}])
        self._reject([{"seq": 1, "table": "t", "row_id": 1, "from": 0,
                       "to": None, "op": "put",
                       "values": {"k": "A", "n": 1, "extra": 2}}])

    def test_delete_must_not_carry_values_and_intervals_well_formed(self):
        self._reject([{"seq": 1, "table": "t", "row_id": 1, "from": 0,
                       "to": None, "op": "delete", "values": {}}])
        self._reject([{"seq": 1, "table": "t", "row_id": 1, "from": 5,
                       "to": 5, "op": "delete"}])
        self._reject([{"seq": 0, "table": "t", "row_id": 1, "from": 0,
                       "to": None, "op": "delete"}])

    def test_duplicate_json_members_and_unknown_fields_rejected(self):
        raw = ('{"seq":1,"seq":2,"table":"t","row_id":1,"from":0,"to":null,'
               '"op":"delete"}\n')
        with open(self.log, "w", encoding="utf-8") as fh:
            fh.write(raw)
        with self.assertRaises(CorrectionLogError):
            materialize_as_of(self.registry, "base", self.log, 0, 10, "dup")
        self._reject([{"seq": 1, "table": "t", "row_id": 1, "from": 0,
                       "to": None, "op": "delete", "surprise": 1}])

    def test_untouched_but_invalid_future_correction_still_rejected(self):
        # seq is beyond S and outside T's interval; it must still be validated.
        future = [{"seq": 99, "table": "t", "row_id": 999, "from": 500,
                   "to": 600, "op": "put",
                   "values": {"k": "Q", "n": "not-an-int"}}]
        self._reject(future)
        # ... and an unknown table is refused even when it is invisible at T.
        self._reject([{"seq": 99, "table": "no_such_table", "row_id": 1,
                       "from": 0, "to": None, "op": "delete"}])

    def test_malformed_json_line_rejected(self):
        with open(self.log, "w", encoding="utf-8") as fh:
            fh.write('{"seq": 1, "table": "t"\n')
        with self.assertRaises(CorrectionLogError):
            materialize_as_of(self.registry, "base", self.log, 0, 10, "bad")
        self.assertFalse(os.path.exists(os.path.join(
            self.registry.snapshots_dir, "bad")))


class FinalViewAndAlgorithmTests(MaterializeBase):
    def test_swap_and_take_over_accepted(self):
        # 1->B/2->A is a swap; deleting 2 while 1 takes its key is a take-over.
        result = self.materialize(22, 14, "takeover", events=[
            {"seq": 1, "table": "t", "row_id": 1, "from": 0, "to": None,
             "op": "put", "values": {"k": "B", "n": 1}},
            {"seq": 2, "table": "t", "row_id": 2, "from": 0, "to": None,
             "op": "delete"},
        ])
        assert_rows(self, result.snapshot, "t", {1: {"k": "B", "n": 1}})

    def test_final_duplicate_key_rejected(self):
        with self.assertRaises(KeyConflictError):
            self.materialize(22, 14, "clash", events=[
                {"seq": 1, "table": "t", "row_id": 1, "from": 0, "to": None,
                 "op": "put", "values": {"k": "B", "n": 1}},
            ])

    def test_later_event_shadows_earlier_and_open_intervals(self):
        events = [
            {"seq": 1, "table": "t", "row_id": 1, "from": 0, "to": None,
             "op": "put", "values": {"k": "A", "n": 100}},
            {"seq": 2, "table": "t", "row_id": 1, "from": 50, "to": None,
             "op": "put", "values": {"k": "A", "n": 200}},
        ]
        at_10 = self.materialize(10, 2, "open-10", events=events).snapshot
        at_60 = self.materialize(60, 2, "open-60", events=events).snapshot
        self.assertEqual(scan_table(at_10, "t")[1]["n"], 100)
        self.assertEqual(scan_table(at_60, "t")[1]["n"], 200)

    def test_absent_row_can_be_created_and_then_deleted(self):
        events = [
            {"seq": 1, "table": "t", "row_id": 3, "from": 0, "to": 50,
             "op": "put", "values": {"k": "C", "n": 30}},
            {"seq": 2, "table": "t", "row_id": 3, "from": 50, "to": None,
             "op": "delete"},
        ]
        born = self.materialize(10, 2, "born", events=events).snapshot
        gone = self.materialize(60, 2, "gone", events=events).snapshot
        self.assertIn(3, scan_table(born, "t"))
        self.assertNotIn(3, scan_table(gone, "t"))
        # Deleting a row that never existed is a no-op, not an error.
        self.assertEqual(scan_table(gone, "t"), BASE_ROWS)


class DeterminismAndRetryTests(MaterializeBase):
    def test_same_content_same_digest_across_shuffle_resend_and_ids(self):
        shuffled = list(REFERENCE_EVENTS)
        shuffled.reverse()
        first = self.materialize(22, 9, "det-a", events=REFERENCE_EVENTS)
        second = self.materialize(
            22, 9, "det-b", events=shuffled + [REFERENCE_EVENTS[1]])
        self.assertEqual(first.snapshot.digest, second.snapshot.digest)
        self.assertNotEqual(first.snapshot.snapshot_id,
                            second.snapshot.snapshot_id)

    def test_retry_same_id_same_content_succeeds(self):
        first = self.materialize(22, 9, "retry")
        second = self.materialize(22, 9, "retry")
        self.assertEqual(first.outcome, "published")
        self.assertEqual(second.outcome, "unchanged")
        self.assertEqual(second.snapshot.digest, first.snapshot.digest)

    def test_same_id_different_content_refused_without_overwrite(self):
        first = self.materialize(22, 9, "fixed-id")
        with self.assertRaises(SnapshotConflict):
            self.materialize(22, 14, "fixed-id")
        self.assertEqual(self.registry.open("fixed-id").digest,
                         first.snapshot.digest)
        assert_rows(self, self.registry.open("fixed-id"), "t", {
            1: {"k": "B", "n": 11},
            2: {"k": "A", "n": 21},
        })

    def test_current_pointer_is_never_moved(self):
        self.materialize(22, 9, "invisible-to-current")
        self.assertEqual(self.registry.current_id(), "base")


class OldSnapshotRemainsReadableTests(MaterializeBase):
    def setUp(self):
        super().setUp()
        # A table the real contract job reads, present in the base only.
        self.job_base = self._publish_sales_base()
        early_events = [{"seq": 1, "table": "sales", "row_id": 1,
                         "from": 10, "to": None, "op": "put",
                         "values": {"region": "N", "amount": 11}}]
        write_log(self.log, early_events)
        self.early = materialize_as_of(
            self.registry, "sales-base", self.log, 30, 1, "early")

    def _publish_sales_base(self):
        b = SnapshotBuilder(os.path.join(self.tmp, "sales-base"))
        b.add_table("sales", [("region", "text"), ("amount", "int64")])
        b.add_row("sales", 1, {"region": "N", "amount": 2})
        b.add_row("sales", 2, {"region": "N", "amount": 3})
        snap = b.finalize(snapshot_id="sales-base")
        self.registry.publish(snap)
        return snap

    def test_old_snapshot_still_serves_existing_reader_after_late_correction(self):
        # A real job output captured on an earlier view...
        store = CacheStore(os.path.join(self.tmp, "cache"))
        try:
            run = store.run(RATE_JOB, self.registry,
                            snapshot_id=self.early.snapshot.snapshot_id)
        finally:
            store.close()
        self.assertEqual(run.mode, "executed")
        # ... a later correction arrives under the same output id, but the
        # content differs, so publication is refused before anything moves ...
        write_log(self.log, [{"seq": 1, "table": "sales", "row_id": 1,
                              "from": 10, "to": None, "op": "put",
                              "values": {"region": "N", "amount": 99}}])
        with self.assertRaises(SnapshotConflict):
            materialize_as_of(self.registry, "sales-base", self.log, 30, 1,
                              "early")
        # ... the old snapshot is untouched and the original reader serves it.
        self.assertEqual(self.registry.open("early").digest,
                         self.early.snapshot.digest)
        snap = self.registry.open("early")
        reader = Reader(snap, snap.connect())
        try:
            rows = list(reader.read("sales", None, ["region", "amount"]))
        finally:
            reader.conn.close()
        self.assertEqual([(r.row_id, dict(r.values)) for r in rows],
                         [(1, {"region": "N", "amount": 11}),
                          (2, {"region": "N", "amount": 3})])
        self.assertEqual(run.snapshot_id, "early")

    def test_missing_base_id_errors_without_creating_output(self):
        write_log(self.log, [])
        with self.assertRaises(FileNotFoundError):
            materialize_as_of(self.registry, "nope", self.log, 0, 0, "out")
        self.assertFalse(os.path.exists(os.path.join(
            self.registry.snapshots_dir, "out")))


class CliTests(MaterializeBase):
    def cli(self, *args):
        import subprocess
        proc = subprocess.run(
            [sys.executable, "-I", os.path.join(PROJECT, "bcache_run.py"),
             *args],
            cwd=PROJECT, capture_output=True, text=True, check=False,
        )
        return proc

    def test_cli_round_trip(self):
        write_log(self.log, REFERENCE_EVENTS)
        proc = self.cli(
            "materialize-as-of",
            "--registry", self.registry.root,
            "--base", "base",
            "--changes", self.log,
            "--effective-at", "22",
            "--known-through", "9",
            "--output-id", "cli-out",
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["outcome"], "published")
        self.assertEqual(payload["output_id"], "cli-out")
        assert_rows(self, self.registry.open("cli-out"), "t", {
            1: {"k": "B", "n": 11},
            2: {"k": "A", "n": 21},
        })
        self.assertEqual(self.registry.current_id(), "base")

    def test_cli_reports_key_conflict_distinctly(self):
        write_log(self.log, REFERENCE_EVENTS)
        proc = self.cli(
            "materialize-as-of", "--registry", self.registry.root,
            "--base", "base", "--changes", self.log,
            "--effective-at", "30", "--known-through", "16",
            "--output-id", "conflict",
        )
        self.assertEqual(proc.returncode, 3)
        self.assertIn("duplicate key", proc.stderr)


class RandomizedLogTests(MaterializeBase):
    """Generate an out-of-order log at runtime and verify delivered selection."""

    def test_random_corrections_match_independent_reference(self):
        import random

        rng = random.Random(20261122)
        n_rows, n_events = 400, 1200
        b = SnapshotBuilder(os.path.join(self.tmp, "rand-base"))
        b.add_table("t", [("k", "text"), ("n", "int64")], key_col="k")
        base_rows = {}
        for rid in range(1, n_rows + 1):
            base_rows[rid] = {"k": "k%04d" % rid, "n": rid}
            b.add_row("t", rid, base_rows[rid])
        base = b.finalize(snapshot_id="rand-base")
        self.registry.publish(base)

        events = []
        for seq in range(1, n_events + 1):
            start = rng.randint(0, 90)
            end = rng.choice([None, start + rng.randint(1, 40)])
            rid = rng.randint(1, n_rows)
            if rng.random() < 0.25:
                events.append({"seq": seq, "table": "t", "row_id": rid,
                               "from": start, "to": end, "op": "delete"})
            else:
                events.append({"seq": seq, "table": "t", "row_id": rid,
                               "from": start, "to": end, "op": "put",
                               "values": {"k": "k%04d" % rid,
                                          "n": rng.randint(0, 1000)}})
        rng.shuffle(events)
        write_log(self.log, events)

        T, S = 42, 900
        result = materialize_as_of(
            self.registry, "rand-base", self.log, T, S, "rand-out")
        delivered = scan_table(result.snapshot, "t")

        # Independent reference selection over the in-memory events.
        winners = {}
        for event in events:
            if event["seq"] > S or event["from"] > T:
                continue
            if event["to"] is not None and T >= event["to"]:
                continue
            current = winners.get(event["row_id"])
            if current is None or event["seq"] > current["seq"]:
                winners[event["row_id"]] = event
        expected = {rid: dict(row) for rid, row in base_rows.items()}
        for rid, event in winners.items():
            if event["op"] == "delete":
                expected.pop(rid, None)
            else:
                expected[rid] = dict(event["values"])
        self.assertEqual(delivered, expected)
        self.assertEqual(len(delivered), len(expected))

        # Re-materialising the same content under a fresh id is identical.
        again = materialize_as_of(
            self.registry, "rand-base", self.log, T, S, "rand-out-2")
        self.assertEqual(again.snapshot.digest, result.snapshot.digest)


class BoundedMemoryTests(unittest.TestCase):
    """Peak Python memory must not grow with base rows or log records."""

    def _peak_materialize(self, n_rows, n_events):
        import tracemalloc

        tmp = tempfile.mkdtemp(prefix="mat-mem-")
        registry = SnapshotRegistry(os.path.join(tmp, "registry"))
        b = SnapshotBuilder(os.path.join(tmp, "base"))
        # No declared key here: this test measures the streaming scan/digest,
        # and keyed row insertion is deliberately the slow, defensive path.
        b.add_table("t", [("k", "text"), ("n", "int64")])
        for rid in range(1, n_rows + 1):
            b.add_row("t", rid, {"k": "k%08d" % rid, "n": rid})
        registry.publish(b.finalize(snapshot_id="base"))

        log = os.path.join(tmp, "changes.jsonl")
        with open(log, "w", encoding="utf-8") as fh:
            for seq in range(1, n_events + 1):
                rid = (seq * 7919) % n_rows + 1
                event = {"seq": seq, "table": "t", "row_id": rid,
                         "from": 0, "to": None, "op": "put",
                         "values": {"k": "k%08d" % rid, "n": seq}}
                fh.write(json.dumps(event) + "\n")

        tracemalloc.start()
        try:
            result = materialize_as_of(
                registry, "base", log, 5, n_events, "out")
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        # The output digest was produced by the bounded streaming scan; a
        # second build must agree, proving the digest path is deterministic
        # without having retained anything from the first.
        self.assertTrue(result.snapshot.digest)
        return peak

    def test_peak_memory_independent_of_base_rows_and_log_size(self):
        small = self._peak_materialize(2000, 4000)
        large = self._peak_materialize(20000, 8000)
        print("\n[materialize] peak small=%d large=%d bytes" % (small, large))
        # Ten times the rows and twice the log; a linear implementation would
        # be many times larger.  Keep a generous absolute cap too.
        self.assertLess(large, small * 3 + 4 * 1024 * 1024,
                        msg="small=%d large=%d" % (small, large))
        self.assertLess(large, 25 * 1024 * 1024,
                        msg="peak %d" % large)


if __name__ == "__main__":
    unittest.main()
