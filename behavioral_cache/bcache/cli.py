"""Command line entries: run / inspect / why-recompute / verify-result."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .jobs import build_identity
from .snapshot import SnapshotRegistry
from .store import CacheStore


def _load_params(text: str | None) -> Any:
    if text is None:
        return None
    return json.loads(text)


def _print(obj: Any) -> None:
    print(json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="bcache",
        description="Read-behaviour-validated offline processing runner",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run a job, reusing or rebuilding cache")
    p_run.add_argument("job_module")
    p_run.add_argument("--registry", required=True)
    p_run.add_argument("--cache", required=True)
    p_run.add_argument("--params")
    p_run.add_argument("--snapshot-id")
    p_run.add_argument("--runtime-id")

    p_why = sub.add_parser("why-recompute", help="explain a reuse/recompute decision")
    p_why.add_argument("job_module")
    p_why.add_argument("--registry", required=True)
    p_why.add_argument("--cache", required=True)
    p_why.add_argument("--params")
    p_why.add_argument("--snapshot-id")
    p_why.add_argument("--runtime-id")

    p_ins = sub.add_parser("inspect", help="inspect one registered cache entry")
    p_ins.add_argument("entry_id")
    p_ins.add_argument("--cache", required=True)

    p_ver = sub.add_parser("verify-result", help="verify package and evidence chain")
    p_ver.add_argument("entry_id")
    p_ver.add_argument("--cache", required=True)

    args = parser.parse_args(argv)
    store = CacheStore(args.cache)
    try:
        if args.command == "run":
            registry = SnapshotRegistry(args.registry)
            result = store.run(
                args.job_module,
                registry,
                params=_load_params(args.params),
                snapshot_id=args.snapshot_id,
                runtime_id=args.runtime_id,
            )
            _print(
                {
                    "mode": result.mode,
                    "entry_id": result.entry_id,
                    "snapshot_id": result.snapshot_id,
                    "job_version": result.job_version,
                    "launches": result.launches,
                    "output_digest": result.output.digest,
                    "rows": [dict(r) for r in result.output.rows],
                    "replay": result.replay.as_dict() if result.replay else None,
                }
            )
            return 0

        if args.command == "why-recompute":
            registry = SnapshotRegistry(args.registry)
            report = store.why_recompute(
                args.job_module,
                registry,
                params=_load_params(args.params),
                snapshot_id=args.snapshot_id,
                runtime_id=args.runtime_id,
            )
            _print(
                {
                    "reusable": report.reusable,
                    "reason": report.reason,
                    "candidate_entry_id": report.candidate_entry_id,
                    "candidates_checked": report.candidates_checked,
                    "replay": report.replay.as_dict() if report.replay else None,
                }
            )
            return 0

        if args.command == "inspect":
            _print(store.inspect(args.entry_id))
            return 0

        if args.command == "verify-result":
            _print(store.verify_result(args.entry_id))
            return 0

        parser.error("unknown command")
        return 2
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
