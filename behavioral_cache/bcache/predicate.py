"""Typed predicates with SQL-style three-valued logic.

A predicate is an immutable nested tuple, which makes its canonical form just
:func:`bcache.encoding.canonical_json`:

* comparison:  ``(op, column_name, value)`` with op in
  ``eq ne gt ge lt le``; the value is int64 / str / bool / None
* null tests:  ``("is_null", col)`` / ``("not_null", col)``
* junctions:   ``("and", *children)`` / ``("or", *children)`` /
  ``("not", child)``

Evaluation returns ``True`` / ``False`` / ``None`` where ``None`` means the
SQL "unknown" state.  Booleans and integers are deliberately *not* equal:
``eq("c", True)`` never matches the integer ``1``.
"""

from __future__ import annotations

from typing import Any, Sequence

from .encoding import MISSING, canonical_json

OPS = {"eq", "ne", "gt", "ge", "lt", "le"}
LEAF_OPS = OPS | {"is_null", "not_null"}


class PredicateError(ValueError):
    """Malformed predicate or reference to an unknown column."""


# ---------------------------------------------------------------------------
# Constructors
# ---------------------------------------------------------------------------

def _column(col: str) -> str:
    if not isinstance(col, str) or not col:
        raise PredicateError("column name must be a non-empty string")
    return col


def _scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        if isinstance(value, int) and not isinstance(value, bool):
            if not (-(2**63) <= value <= 2**63 - 1):
                raise PredicateError("integer outside int64 range")
        return value
    raise PredicateError("unsupported predicate literal: %r" % (type(value),))


def eq(col: str, value: Any) -> tuple:
    col = _column(col)
    value = _scalar(value)
    if value is None:
        return ("is_null", col)
    return ("eq", col, value)


def ne(col: str, value: Any) -> tuple:
    col = _column(col)
    value = _scalar(value)
    if value is None:
        return ("not_null", col)
    return ("ne", col, value)


def gt(col: str, value: Any) -> tuple:
    return ("gt", _column(col), _scalar(value))


def ge(col: str, value: Any) -> tuple:
    return ("ge", _column(col), _scalar(value))


def lt(col: str, value: Any) -> tuple:
    return ("lt", _column(col), _scalar(value))


def le(col: str, value: Any) -> tuple:
    return ("le", _column(col), _scalar(value))


def is_null(col: str) -> tuple:
    return ("is_null", _column(col))


def not_null(col: str) -> tuple:
    return ("not_null", _column(col))


def and_(*children: tuple) -> tuple:
    if not children:
        raise PredicateError("and requires at least one child")
    return ("and",) + tuple(children)


def or_(*children: tuple) -> tuple:
    if not children:
        raise PredicateError("or requires at least one child")
    return ("or",) + tuple(children)


def not_(child: tuple) -> tuple:
    return ("not", child)


# ---------------------------------------------------------------------------
# Validation and canonical form
# ---------------------------------------------------------------------------

def referenced_columns(pred: tuple) -> set[str]:
    if not isinstance(pred, tuple) or not pred:
        raise PredicateError("predicate must be a non-empty tuple")
    op = pred[0]
    if op in LEAF_OPS:
        if op in ("is_null", "not_null"):
            if len(pred) != 2:
                raise PredicateError("%s takes one column" % op)
            return {_column(pred[1])}
        if len(pred) != 3:
            raise PredicateError("%s takes a column and a value" % op)
        if op not in OPS:
            raise PredicateError("unknown op %r" % (op,))
        _column(pred[1])
        _scalar(pred[2])
        return {pred[1]}
    if op == "not":
        if len(pred) != 2:
            raise PredicateError("not takes one child")
        return referenced_columns(pred[1])
    if op in ("and", "or"):
        cols: set[str] = set()
        for child in pred[1:]:
            cols |= referenced_columns(child)
        return cols
    raise PredicateError("unknown predicate node %r" % (op,))


def validate(pred: tuple, known_columns: Sequence[str]) -> None:
    """Raise :class:`PredicateError` if the predicate names an unknown column."""
    known = set(known_columns)
    for col in referenced_columns(pred):
        if col not in known:
            raise PredicateError("unknown column %r" % (col,))


def canonical(pred: tuple) -> bytes:
    # Structural validation happens as a side effect.
    referenced_columns(pred)
    return canonical_json(pred)


# ---------------------------------------------------------------------------
# Three-valued evaluation
# ---------------------------------------------------------------------------

def _same_family(a: Any, b: Any) -> bool:
    """Whether two non-null literals share a comparable type domain."""
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool)
    if isinstance(a, int) and isinstance(b, int):
        return True
    return type(a) is type(b)


def _compare(op: str, a: Any, b: Any) -> bool | None:
    # Null on either side -> unknown.
    if a is None or b is None:
        return None
    if op in ("eq", "ne"):
        if not _same_family(a, b):
            return False if op == "eq" else True
        equal = a == b
        return equal if op == "eq" else not equal
    if not _same_family(a, b):
        # Ordering across type domains is undefined -> unknown.
        return None
    if op == "gt":
        return a > b
    if op == "ge":
        return a >= b
    if op == "lt":
        return a < b
    if op == "le":
        return a <= b
    raise PredicateError("unknown comparison %r" % (op,))


def evaluate(pred: tuple, row: dict) -> bool | None:
    """Evaluate against a row mapping.

    A column that exists in the schema but is absent/NULL in the row maps to
    ``None``.  A column that is not part of the row mapping at all is also
    treated as NULL here; schema validation happens before evaluation.
    """
    op = pred[0]
    if op in LEAF_OPS:
        col = pred[1]
        value = row.get(col, None)
        if value is MISSING:
            value = None
        if op == "is_null":
            return value is None
        if op == "not_null":
            return value is not None
        return _compare(op, value, pred[2])
    if op == "not":
        inner = evaluate(pred[1], row)
        return None if inner is None else not inner
    if op == "and":
        saw_unknown = False
        for child in pred[1:]:
            result = evaluate(child, row)
            if result is False:
                return False
            if result is None:
                saw_unknown = True
        return None if saw_unknown else True
    if op == "or":
        saw_unknown = False
        for child in pred[1:]:
            result = evaluate(child, row)
            if result is True:
                return True
            if result is None:
                saw_unknown = True
        return None if saw_unknown else False
    raise PredicateError("unknown predicate node %r" % (op,))
