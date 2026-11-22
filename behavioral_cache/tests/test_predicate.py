"""Predicate tests: three-valued null logic, type domains, range boundaries."""

import unittest

from bcache import and_, eq, ge, gt, is_null, le, lt, ne, not_, not_null, or_
from bcache.encoding import MISSING
from bcache.predicate import PredicateError, evaluate, validate


class PredicateTests(unittest.TestCase):
    def test_eq_three_valued_null(self):
        self.assertIs(evaluate(eq("x", 5), {"x": None}), None)
        self.assertIs(evaluate(eq("x", 5), {"x": MISSING}), None)
        self.assertTrue(evaluate(eq("x", 5), {"x": 5}))
        self.assertFalse(evaluate(eq("x", 5), {"x": 6}))

    def test_is_null_vs_not_null(self):
        self.assertTrue(evaluate(is_null("x"), {"x": None}))
        self.assertTrue(evaluate(is_null("x"), {"x": MISSING}))
        self.assertFalse(evaluate(not_null("x"), {"x": None}))
        self.assertTrue(evaluate(not_null("x"), {"x": 0}))
        # 0 is not NULL even though it is falsy in Python.
        self.assertFalse(evaluate(is_null("x"), {"x": 0}))

    def test_bool_not_equal_to_int(self):
        self.assertFalse(evaluate(eq("x", True), {"x": 1}))
        self.assertFalse(evaluate(eq("x", 1), {"x": True}))
        self.assertTrue(evaluate(eq("x", True), {"x": True}))
        self.assertTrue(evaluate(ne("x", 1), {"x": True}))

    def test_range_boundaries(self):
        pred = and_(ge("x", 10), lt("x", 20))
        self.assertFalse(evaluate(pred, {"x": 9}))
        self.assertTrue(evaluate(pred, {"x": 10}))   # closed lower bound
        self.assertTrue(evaluate(pred, {"x": 19}))
        self.assertFalse(evaluate(pred, {"x": 20}))  # open upper bound
        self.assertIs(evaluate(pred, {"x": None}), None)

    def test_and_or_not_truth_tables(self):
        t, f, u = True, False, None
        self.assertIs(evaluate(and_(eq("a", 1), eq("b", 2)),
                               {"a": 1, "b": None}), None)
        self.assertFalse(evaluate(and_(eq("a", 1), eq("b", 2)),
                                  {"a": 0, "b": None}))
        self.assertTrue(evaluate(or_(eq("a", 1), eq("b", 2)),
                                 {"a": 1, "b": None}))
        self.assertIs(evaluate(or_(eq("a", 1), eq("b", 2)),
                               {"a": 0, "b": None}), None)
        self.assertIs(evaluate(not_(eq("a", 1)), {"a": None}), None)
        self.assertTrue(evaluate(not_(eq("a", 1)), {"a": 2}))
        # Guard against unused-variable style linters on t/f/u.
        self.assertTrue(t and not f and u is None)

    def test_unknown_column_rejected(self):
        with self.assertRaises(PredicateError):
            validate(eq("missing", 1), ["x"])

    def test_cross_family_ordering_is_unknown(self):
        # Ordering text against int is undefined -> unknown, not an error.
        self.assertIs(evaluate(gt("x", 5), {"x": "5"}), None)
        self.assertTrue(evaluate(ne("x", 5), {"x": "5"}))


if __name__ == "__main__":
    unittest.main()
