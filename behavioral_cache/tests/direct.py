"""Independent acceptance helpers.

The acceptance suite never asks the product whether a cache entry is valid.
Instead it:

1. runs the job module directly against a plain reader with caching disabled,
   producing the business baseline, and
2. builds a *hand-written* observation checklist per scenario and compares the
   checklists itself, structurally, then checks the product's reuse/recompute
   decision against that independently derived expectation.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
from typing import Any

from bcache import MISSING, OutputSink, Reader, eq
from bcache.output import package_from_db

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ---------------------------------------------------------------------------
# Cache-disabled real execution (business baseline)
# ---------------------------------------------------------------------------

def load_module(module_path: str):
    spec = importlib.util.spec_from_file_location(
        "acceptance_job_%s" % os.path.basename(module_path)[:-3],
        os.path.abspath(module_path),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def direct_run(module_path: str, snapshot, params=None) -> dict[str, Any]:
    """Run job code straight against a snapshot; no cache store involved."""
    work = tempfile.mkdtemp()
    sink = OutputSink(os.path.join(work, "out.sqlite3"))
    reader = Reader(snapshot, snapshot.connect())
    module = load_module(module_path)
    try:
        module.run(reader, sink, params)
        schema, digest, row_count = sink.close()
        package = package_from_db(
            os.path.join(work, "out.sqlite3"), os.path.join(work, "out.bpkg")
        )
    finally:
        reader.conn.close()
    return {
        "rows": [dict(r) for r in package.rows],
        "digest": package.digest,
        "schema": list(package.schema),
        "row_count": row_count,
    }


# ---------------------------------------------------------------------------
# Hand-written structural comparison (NOT the product replayer)
# ---------------------------------------------------------------------------

def manual_equal(a: Any, b: Any) -> bool:
    """Independent structural equality with the project's type distinctions."""
    if a is MISSING or b is MISSING:
        return a is MISSING and b is MISSING
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, dict) and isinstance(b, dict):
        return set(a) == set(b) and all(manual_equal(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(manual_equal(x, y) for x, y in zip(a, b))
    if type(a) is not type(b):
        return False
    return a == b


# ---------------------------------------------------------------------------
# Hand-written observation checklists for the two contract jobs
# ---------------------------------------------------------------------------

def rate_sales_checklist(snapshot) -> list:
    """Exactly the observations jobs/rate_sales.py performs, in call order."""
    reader = Reader(snapshot, snapshot.connect())
    checklist: list = []
    try:
        rates_exists = reader.exists("rates")
        checklist.append(("exists", "rates", rates_exists))
        if rates_exists:
            result = reader.lookup("rates", "A", key_col="code")
            cell = MISSING
            if result.status == "found":
                cell = result.value["value"]
            checklist.append((
                "lookup", "rates", "code", "A",
                result.status, list(result.schema), cell,
            ))
        rows = list(reader.read("sales", eq("region", "N"), ["amount"]))
        checklist.append((
            "read", "sales",
            list(r.schema for r in rows)[0] if rows else _sales_schema(reader),
            [(r.row_id, dict(r.values)) for r in rows],
        ))
        return checklist
    finally:
        reader.conn.close()


def _sales_schema(reader: Reader):
    snap = reader.snapshot
    if snap.has_table(reader.conn, "sales"):
        schema = snap.schema(reader.conn, "sales")
        return [(n, t) for n, t in schema if n == "amount"]
    return []


def partition_checklist(snapshot, prefix: str = "daily/") -> list:
    """Exactly the observations jobs/daily_parts.py performs, in call order."""
    reader = Reader(snapshot, snapshot.connect())
    checklist: list = []
    try:
        names = reader.list_partitions(prefix)
        checklist.append(("list_partitions", prefix, list(names)))
        for name in names:
            rows = list(reader.read(name, None, ["amount"]))
            checklist.append((
                "read", name,
                list(rows[0].schema) if rows else snap_schema(reader, name),
                [(r.row_id, dict(r.values)) for r in rows],
            ))
        return checklist
    finally:
        reader.conn.close()


def snap_schema(reader: Reader, table: str):
    return [(n, t) for n, t in reader.snapshot.schema(reader.conn, table)
            if n == "amount"]


def expected_decision(base_checklist: list, new_checklist: list) -> str:
    """The acceptance's own reuse/recompute verdict."""
    return "reused" if manual_equal(base_checklist, new_checklist) else "executed"
