"""Runtime generator for large snapshots.

Nothing large is committed to git: this script builds a million-row snapshot
on demand under a caller-provided output directory and prints its manifest.

Usage::

    python3 -I tests/generate_large.py --root /tmp/bcache-large --rows 1000000
"""

from __future__ import annotations

import argparse
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from bcache import SnapshotBuilder, SnapshotRegistry  # noqa: E402


def build_large_snapshot(path: str, rows: int, batch: int = 10000):
    """Create a snapshot with ``rows`` sales rows and a small rates table."""
    builder = SnapshotBuilder(path)
    builder.add_table(
        "rates", [("code", "text"), ("value", "int64")], key_col="code"
    )
    builder.add_row("rates", 1, {"code": "A", "value": 2})
    builder.add_table(
        "sales",
        [("region", "text"), ("amount", "int64"), ("note", "text")],
    )
    values = []
    for row_id in range(1, rows + 1):
        region = "N" if row_id % 2 == 0 else "S"
        values.append((row_id, region, row_id % 1000))
        if len(values) >= batch:
            _flush(builder, values, with_note=False)
            values.clear()
    if values:
        _flush(builder, values, with_note=False)
    return builder.finalize()


def _flush(builder: SnapshotBuilder, rows, with_note: bool) -> None:
    for row_id, region, amount in rows:
        payload = {"region": region, "amount": amount}
        if with_note:
            payload["note"] = "n-%d" % row_id
        builder.add_row("sales", row_id, payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="registry root")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--publish", action="store_true",
                        help="also publish the snapshot to the registry")
    args = parser.parse_args(argv)

    staging = os.path.join(args.root, "staging-large")
    os.makedirs(staging, exist_ok=True)
    started = time.time()
    snap = build_large_snapshot(staging, args.rows)
    elapsed = time.time() - started
    print("snapshot_id=%s digest=%s rows=%d build_seconds=%.2f"
          % (snap.snapshot_id, snap.digest, args.rows, elapsed))

    if args.publish:
        registry = SnapshotRegistry(args.root)
        registry.publish(snap)
        print("published current=%s" % registry.current_id())
    return 0


if __name__ == "__main__":
    sys.exit(main())
