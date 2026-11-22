"""Small in-test snapshot fixtures and builders.

Nothing large lives in git; big snapshots are produced by ``generate_large.py``.
"""

from __future__ import annotations

import os
from typing import Any, Optional

from bcache import Snapshot, SnapshotBuilder, SnapshotRegistry

SALES_SCHEMA = [
    ("row_id_alias", "int64"),  # placeholder removed below
]


def build_sales_snapshot(
    path: str,
    n_amounts: list[int],
    s_amounts: Optional[list[int]] = None,
    remarks: Optional[dict[int, str]] = None,
    rates: Optional[dict[str, int]] = None,
    extra_sales: Optional[list[dict[str, Any]]] = None,
) -> Snapshot:
    """Build the rate/sales scenario family.

    ``n_amounts`` are region='N' amounts with row_ids 1..k; ``s_amounts`` are
    region='S' rows (never matched by the predicate); ``remarks`` adds an
    unprojected text column when supplied; ``rates`` adds the supplementary
    keyed table.
    """
    sales_schema = [
        ("region", "text"),
        ("amount", "int64"),
    ]
    if remarks is not None:
        sales_schema.append(("remark", "text"))
    builder = SnapshotBuilder(path)
    if rates is not None:
        builder.add_table(
            "rates", [("code", "text"), ("value", "int64")], key_col="code"
        )
        for i, (code, value) in enumerate(rates.items(), start=1):
            builder.add_row("rates", i, {"code": code, "value": value})
    builder.add_table("sales", sales_schema)
    row_id = 0
    for amount in n_amounts:
        row_id += 1
        values: dict[str, Any] = {"region": "N", "amount": amount}
        if remarks is not None and row_id in remarks:
            values["remark"] = remarks[row_id]
        builder.add_row("sales", row_id, values)
    if s_amounts:
        for amount in s_amounts:
            row_id += 1
            builder.add_row("sales", row_id, {"region": "S", "amount": amount})
    if extra_sales:
        for values in extra_sales:
            row_id += 1
            builder.add_row("sales", row_id, values)
    return builder.finalize()


def build_partition_snapshot(
    path: str,
    partition_amounts: dict[str, list[int]],
    prefix: str = "daily/",
) -> Snapshot:
    builder = SnapshotBuilder(path)
    for name, amounts in partition_amounts.items():
        builder.add_table(prefix + name, [("amount", "int64")])
        for i, amount in enumerate(amounts, start=1):
            builder.add_row(prefix + name, i, {"amount": amount})
    return builder.finalize()


def publish(registry: SnapshotRegistry, snapshot: Snapshot) -> Snapshot:
    registry.publish(snapshot)
    return snapshot


def fresh_registry(root: str) -> SnapshotRegistry:
    return SnapshotRegistry(os.path.join(root, "registry"))
