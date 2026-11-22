"""Encoding tests: domain tags must prevent every required collision."""

import unittest

from bcache.encoding import (
    MISSING,
    canonical_json,
    digest_values,
    encode_record,
    encode_tuple,
    encode_value,
)


class EncodingTests(unittest.TestCase):
    def test_bool_distinct_from_int(self):
        self.assertNotEqual(encode_value(True), encode_value(1))
        self.assertNotEqual(encode_value(False), encode_value(0))
        self.assertNotEqual(digest_values([True]), digest_values([1]))

    def test_null_distinct_from_missing(self):
        self.assertNotEqual(encode_value(None), encode_value(MISSING))

    def test_text_distinct_from_int(self):
        # "1" vs 1 must never hash equal.
        self.assertNotEqual(encode_value("1"), encode_value(1))
        self.assertNotEqual(digest_values(["1"]), digest_values([1]))

    def test_concatenated_strings_do_not_collide(self):
        # Length framing makes ("ab","c") != ("a","bc").
        self.assertNotEqual(encode_tuple(["ab", "c"]), encode_tuple(["a", "bc"]))

    def test_record_missing_vs_null_distinct(self):
        a = encode_record({"x": None})
        b = encode_record({"x": MISSING})
        self.assertNotEqual(a, b)

    def test_record_key_order_independent(self):
        self.assertEqual(
            encode_record({"a": 1, "b": 2}), encode_record({"b": 2, "a": 1})
        )

    def test_int64_bounds_enforced(self):
        encode_value(2**63 - 1)
        encode_value(-(2**63))
        with self.assertRaises(Exception):
            encode_value(2**63)

    def test_canonical_json_stability(self):
        self.assertEqual(
            canonical_json({"b": 1, "a": [1, 2]}),
            canonical_json({"a": [1, 2], "b": 1}),
        )
        self.assertEqual(canonical_json((1, 2)), canonical_json([1, 2]))


if __name__ == "__main__":
    unittest.main()
