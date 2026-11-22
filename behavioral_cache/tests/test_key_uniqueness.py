"""Key-uniqueness contract: a declared key column rejects duplicate values."""

import os
import tempfile
import unittest

from bcache import SnapshotBuilder
from bcache.errors import SchemaError


class KeyUniquenessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _builder(self, name):
        return SnapshotBuilder(os.path.join(self.tmp, name))

    def test_duplicate_text_key_rejected(self):
        b = self._builder("t")
        b.add_table("kv", [("k", "text"), ("v", "int64")], key_col="k")
        b.add_row("kv", 1, {"k": "x", "v": 1})
        with self.assertRaises(SchemaError):
            b.add_row("kv", 2, {"k": "x", "v": 2})

    def test_duplicate_int_and_bool_keys_rejected(self):
        b = self._builder("i")
        b.add_table("ik", [("id", "int64"), ("n", "int64")], key_col="id")
        b.add_row("ik", 1, {"id": 5, "n": 1})
        with self.assertRaises(SchemaError):
            b.add_row("ik", 2, {"id": 5, "n": 2})

        c = self._builder("b")
        c.add_table("flags", [("on", "bool"), ("n", "int64")], key_col="on")
        c.add_row("flags", 1, {"on": True, "n": 1})
        with self.assertRaises(SchemaError):
            c.add_row("flags", 2, {"on": True, "n": 2})

    def test_null_keys_are_distinct_and_derived_keeps_constraint(self):
        b = self._builder("n")
        b.add_table("kv", [("k", "text"), ("v", "int64")], key_col="k")
        b.add_row("kv", 1, {"k": None, "v": 1})
        b.add_row("kv", 2, {"k": None, "v": 2})  # SQL NULLs are distinct
        base = b.finalize()

        d = SnapshotBuilder(os.path.join(self.tmp, "derived"), derive_from=base)
        # NULL keys stay unconstrained on a derived snapshot.
        d.add_row("kv", 3, {"k": None, "v": 3})
        # A concrete duplicate against an existing concrete value fails.
        d.add_row("kv", 4, {"k": "z", "v": 4})
        with self.assertRaises(SchemaError):
            d.add_row("kv", 5, {"k": "z", "v": 5})


if __name__ == "__main__":
    unittest.main()
