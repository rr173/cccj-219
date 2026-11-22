"""Cache store: ownership, evidence registration, determinism contract.

All durable state lives in one SQLite database under the cache root, with a
filesystem advisory lock serialising execution ownership across processes.
The runner pins one snapshot for the whole operation, so a snapshot
published concurrently cannot mix generations into a replay or a run.
"""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Optional

from .encoding import canonical_json
from .evidence import EvidenceWriter
from .jobs import JobIdentity, build_identity, execute_job
from .output import OutputSink, OutputPackage, package_from_db, read_package
from .reader import Reader, RecordingReader
from .replayer import ReplayValidator, ReplayReport
from .snapshot import Snapshot, SnapshotRegistry

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS entries (
    entry_id TEXT PRIMARY KEY,
    job_version TEXT NOT NULL,
    params_json TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    module_digest TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    snapshot_digest TEXT NOT NULL,
    observation_digest TEXT NOT NULL,
    output_digest TEXT NOT NULL,
    output_path TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    created_at REAL NOT NULL,
    sequence_no INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS entries_lookup
    ON entries (job_version, sequence_no);
CREATE TABLE IF NOT EXISTS calls (
    entry_id TEXT NOT NULL, seq INTEGER NOT NULL, method TEXT NOT NULL,
    args_json TEXT NOT NULL, outcome TEXT NOT NULL, error_type TEXT,
    result_digest TEXT NOT NULL, detail_json TEXT NOT NULL,
    PRIMARY KEY (entry_id, seq)
);
CREATE TABLE IF NOT EXISTS read_rows (
    entry_id TEXT NOT NULL, seq INTEGER NOT NULL, row_idx INTEGER NOT NULL,
    row_id INTEGER NOT NULL, projection_json TEXT NOT NULL,
    PRIMARY KEY (entry_id, seq, row_idx)
);
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    job_version TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL,
    status TEXT NOT NULL,
    error TEXT,
    reused_entry_id TEXT,
    new_entry_id TEXT,
    replay_json TEXT,
    launches INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS lease (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    holder TEXT NOT NULL,
    acquired_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS determinism_violations (
    violation_id TEXT PRIMARY KEY,
    job_version TEXT NOT NULL,
    observation_digest TEXT NOT NULL,
    existing_output_digest TEXT NOT NULL,
    conflicting_output_digest TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS seq_store (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
"""


class DeterminismViolation(RuntimeError):
    """Same job version and observations produced different outputs."""

    def __init__(self, violation_id: str, existing: str, conflicting: str):
        super().__init__(
            "determinism contract broken: identical observations produced "
            "different outputs (%s vs %s); violation %s"
            % (existing, conflicting, violation_id)
        )
        self.violation_id = violation_id
        self.existing_output_digest = existing
        self.conflicting_output_digest = conflicting


@dataclass
class RunResult:
    mode: str                     # reused | executed
    entry_id: str
    output: OutputPackage
    snapshot_id: str
    job_version: str
    launches: int                 # real job starts attributable to this run
    replay: Optional[ReplayReport] = None
    run_id: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class WhyReport:
    reusable: bool
    reason: Optional[str] = None
    candidate_entry_id: Optional[str] = None
    replay: Optional[ReplayReport] = None
    candidates_checked: int = 0


class CacheStore:
    def __init__(self, root: str, lease_ttl_seconds: float = 3600.0):
        self.root = os.path.abspath(root)
        self.packages_dir = os.path.join(self.root, "packages")
        self.tmp_dir = os.path.join(self.root, "tmp")
        self.lock_path = os.path.join(self.root, "execution.lock")
        os.makedirs(self.packages_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)
        self.db_path = os.path.join(self.root, "cache.sqlite3")
        self.lease_ttl_seconds = lease_ttl_seconds
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA_SQL)
        self._conn.commit()
        self._holder = "pid-%d-%s" % (os.getpid(), uuid.uuid4().hex[:12])

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ utils

    def _next_seq(self, name: str) -> int:
        row = self._conn.execute(
            "SELECT value FROM seq_store WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO seq_store(name, value) VALUES (?, 1)", (name,)
            )
            value = 1
        else:
            value = row[0] + 1
            self._conn.execute(
                "UPDATE seq_store SET value = ? WHERE name = ?", (value, name)
            )
        return value

    @contextmanager
    def execution_lock(self):
        """Cross-process ownership of execution for this cache store."""
        with open(self.lock_path, "a+") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    def force_release_stale_lease(self) -> None:
        """Test/operator hook: the advisory lock itself is the real guard."""
        self._conn.execute("DELETE FROM lease WHERE id = 1")
        self._conn.commit()

    # ------------------------------------------------------------------ run

    def run(
        self,
        module_path: str,
        registry: SnapshotRegistry,
        params: Any = None,
        snapshot_id: Optional[str] = None,
        runtime_id: Optional[str] = None,
    ) -> RunResult:
        identity = build_identity(module_path, params, runtime_id)
        # Pin the snapshot once; later publishes never alter this handle.
        snapshot = registry.pin(snapshot_id)
        run_id = uuid.uuid4().hex
        self._start_run(run_id, identity, snapshot, "unknown")

        with self.execution_lock():
            candidate = self._latest_entry(identity.version)
            report: Optional[ReplayReport] = None
            if candidate is not None:
                report = self._replay_candidate(candidate, snapshot)
                if report.matched:
                    package = read_package(candidate["output_path"])
                    self._finish_run(
                        run_id, "reused", "ok",
                        reused_entry_id=candidate["entry_id"],
                        replay=report,
                    )
                    return RunResult(
                        mode="reused",
                        entry_id=candidate["entry_id"],
                        output=package,
                        snapshot_id=snapshot.snapshot_id,
                        job_version=identity.version,
                        launches=0,
                        replay=report,
                        run_id=run_id,
                    )

            # Candidate invalid or absent: really launch the job on the same
            # pinned snapshot.  The replay above never ran job code. The run
            # stays "running" until _execute_and_commit finishes it; a raised
            # exception or a killed process therefore can never look like an
            # ok execution.
            result = self._execute_and_commit(
                identity, snapshot, run_id,
                invalidation=report,
            )
            result.run_id = run_id
            return result

    # ------------------------------------------------------------------ replay

    def _replay_candidate(self, entry: sqlite3.Row, snapshot: Snapshot) -> ReplayReport:
        validator = ReplayValidator(snapshot, self._conn, entry["entry_id"])
        try:
            return validator.validate()
        finally:
            validator.close()

    # ------------------------------------------------------------------ execute

    def _execute_and_commit(
        self,
        identity: JobIdentity,
        snapshot: Snapshot,
        run_id: str,
        invalidation: Optional[ReplayReport],
    ) -> RunResult:
        entry_id = uuid.uuid4().hex
        work_dir = os.path.join(self.tmp_dir, entry_id)
        os.makedirs(work_dir, exist_ok=True)
        evidence_path = os.path.join(work_dir, "evidence.sqlite3")
        output_db = os.path.join(work_dir, "output.sqlite3")
        evidence = EvidenceWriter(evidence_path)
        sink = OutputSink(output_db)
        reader = Reader(snapshot, snapshot.connect())
        recording = RecordingReader(reader, evidence)
        launches = 1
        try:
            execute_job(identity.module_path, recording, sink, identity.params)
            schema, output_digest, row_count = sink.close()
            evidence.set_meta("snapshot_id", snapshot.snapshot_id)
            evidence.set_meta("snapshot_digest", snapshot.digest)
            evidence.close()
            reader.conn.close()
        except Exception as exc:  # noqa: BLE001 - failed/killed jobs are not cached
            evidence.close()
            reader.conn.close()
            shutil.rmtree(work_dir, ignore_errors=True)
            self._fail_run(run_id, exc)
            raise

        observation_digest = self._digest_evidence(evidence_path)
        # Determinism contract: same version + same observations must give
        # the same output.  Never silently pick one.
        existing = self._find_observation(identity.version, observation_digest)
        if existing is not None and existing["output_digest"] != output_digest:
            violation_id = uuid.uuid4().hex
            self._conn.execute(
                "INSERT INTO determinism_violations(violation_id, job_version, "
                "observation_digest, existing_output_digest, conflicting_output_digest, "
                "detail_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    violation_id,
                    identity.version,
                    observation_digest,
                    existing["output_digest"],
                    output_digest,
                    canonical_json(
                        {
                            "existing_entry": existing["entry_id"],
                            "failed_entry": entry_id,
                            "module_digest": identity.module_digest,
                        }
                    ).decode(),
                    time.time(),
                ),
            )
            self._conn.commit()
            shutil.rmtree(work_dir, ignore_errors=True)
            violation = DeterminismViolation(
                violation_id, existing["output_digest"], output_digest
            )
            self._fail_run(run_id, violation)
            raise violation

        package_path = os.path.join(self.packages_dir, entry_id + ".bpkg")
        package = package_from_db(output_db, package_path)

        sequence_no = self._next_seq("entry")
        self._conn.execute(
            "INSERT INTO entries(entry_id, job_version, params_json, runtime_id, "
            "module_digest, snapshot_id, snapshot_digest, observation_digest, "
            "output_digest, output_path, row_count, created_at, sequence_no) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entry_id,
                identity.version,
                canonical_json(identity.params).decode(),
                identity.runtime_id,
                identity.module_digest,
                snapshot.snapshot_id,
                snapshot.digest,
                observation_digest,
                output_digest,
                package_path,
                row_count,
                time.time(),
                sequence_no,
            ),
        )
        # Bulk-copy evidence under the already-held lock.
        self._attach_evidence(evidence_path, entry_id)
        self._conn.execute(
            "UPDATE runs SET mode = 'executed', status = 'ok', "
            "new_entry_id = ?, reused_entry_id = COALESCE(reused_entry_id, ''), "
            "launches = 1, finished_at = ? WHERE run_id = ?",
            (entry_id, time.time(), run_id),
        )
        self._conn.commit()
        shutil.rmtree(work_dir, ignore_errors=True)
        return RunResult(
            mode="executed",
            entry_id=entry_id,
            output=package,
            snapshot_id=snapshot.snapshot_id,
            job_version=identity.version,
            launches=launches,
            replay=invalidation,
            details={
                "observation_digest": observation_digest,
                "calls": recording.call_count,
            },
        )

    def _attach_evidence(self, evidence_path: str, entry_id: str) -> None:
        # Stream rows from the per-run evidence database into the store one
        # batch at a time: no ATTACH (which shares locking) and no fetchall.
        ev_conn = sqlite3.connect(evidence_path)
        try:
            call_cursor = ev_conn.execute(
                "SELECT seq, method, args_json, outcome, error_type, "
                "result_digest, detail_json FROM calls ORDER BY seq"
            )
            while True:
                batch = call_cursor.fetchmany(500)
                if not batch:
                    break
                self._conn.executemany(
                    "INSERT INTO calls(entry_id, seq, method, args_json, outcome, "
                    "error_type, result_digest, detail_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [(entry_id, *r) for r in batch],
                )
            row_cursor = ev_conn.execute(
                "SELECT seq, row_idx, row_id, projection_json "
                "FROM read_rows ORDER BY seq, row_idx"
            )
            while True:
                batch = row_cursor.fetchmany(500)
                if not batch:
                    break
                self._conn.executemany(
                    "INSERT INTO read_rows(entry_id, seq, row_idx, row_id, "
                    "projection_json) VALUES (?, ?, ?, ?, ?)",
                    [(entry_id, *r) for r in batch],
                )
        finally:
            ev_conn.close()

    def _digest_evidence(self, evidence_path: str) -> str:
        from .encoding import StreamHasher

        conn = sqlite3.connect(evidence_path)
        conn.row_factory = sqlite3.Row
        hasher = StreamHasher(namespace="observation-log-v1")
        for call in conn.execute(
            "SELECT seq, method, args_json, outcome, error_type, result_digest, "
            "detail_json FROM calls ORDER BY seq"
        ):
            hasher.update_encoded(canonical_json([
                call["seq"], call["method"], json.loads(call["args_json"]),
                call["outcome"], call["error_type"],
                call["result_digest"], json.loads(call["detail_json"]),
            ]))
        conn.close()
        return hasher.hexdigest()

    # ------------------------------------------------------------------ queries

    def _latest_entry(self, job_version: str) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM entries WHERE job_version = ? "
            "ORDER BY sequence_no DESC LIMIT 1",
            (job_version,),
        ).fetchone()

    def _find_observation(
        self, job_version: str, observation_digest: str
    ) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM entries WHERE job_version = ? AND observation_digest = ? "
            "ORDER BY sequence_no DESC LIMIT 1",
            (job_version, observation_digest),
        ).fetchone()

    def inspect(self, entry_id: str) -> dict[str, Any]:
        entry = self._conn.execute(
            "SELECT * FROM entries WHERE entry_id = ?", (entry_id,)
        ).fetchone()
        if entry is None:
            raise KeyError("no cache entry %s" % entry_id)
        calls = self._conn.execute(
            "SELECT seq, method, args_json, outcome, error_type, result_digest, "
            "detail_json FROM calls WHERE entry_id = ? ORDER BY seq",
            (entry_id,),
        ).fetchall()
        return {
            "entry": dict(entry),
            "calls": [dict(c) for c in calls],
            "read_row_total": self._conn.execute(
                "SELECT COUNT(*) FROM read_rows WHERE entry_id = ?", (entry_id,)
            ).fetchone()[0],
        }

    def entries_for_job(self, job_version: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT entry_id, snapshot_id, output_digest, observation_digest, "
            "sequence_no, created_at FROM entries WHERE job_version = ? "
            "ORDER BY sequence_no",
            (job_version,),
        ).fetchall()
        return [dict(r) for r in rows]

    def why_recompute(
        self,
        module_path: str,
        registry: SnapshotRegistry,
        params: Any = None,
        snapshot_id: Optional[str] = None,
        runtime_id: Optional[str] = None,
    ) -> WhyReport:
        """Explain reuse/recompute without executing the job."""
        identity = build_identity(module_path, params, runtime_id)
        snapshot = registry.pin(snapshot_id)
        candidate = self._latest_entry(identity.version)
        if candidate is None:
            return WhyReport(reusable=False, reason="no cache candidate exists")
        report = self._replay_candidate(candidate, snapshot)
        if report.matched:
            return WhyReport(
                reusable=True,
                reason="all recorded observations match",
                candidate_entry_id=candidate["entry_id"],
                replay=report,
                candidates_checked=1,
            )
        return WhyReport(
            reusable=False,
            reason=report.reason,
            candidate_entry_id=candidate["entry_id"],
            replay=report,
            candidates_checked=1,
        )

    def verify_result(self, entry_id: str) -> dict[str, Any]:
        """Check the registered evidence chain and package integrity."""
        entry = self._conn.execute(
            "SELECT * FROM entries WHERE entry_id = ?", (entry_id,)
        ).fetchone()
        if entry is None:
            raise KeyError("no cache entry %s" % entry_id)
        package = read_package(entry["output_path"])
        package_ok = package.digest == entry["output_digest"]
        recomputed_evidence = self._digest_evidence_from_store(entry_id)
        evidence_ok = recomputed_evidence == entry["observation_digest"]
        call_count = self._conn.execute(
            "SELECT COUNT(*) FROM calls WHERE entry_id = ?", (entry_id,)
        ).fetchone()[0]
        return {
            "entry_id": entry_id,
            "package_exists": os.path.exists(entry["output_path"]),
            "package_digest_ok": package_ok,
            "evidence_digest_ok": evidence_ok,
            "output_digest": entry["output_digest"],
            "row_count": package.row_count,
            "schema": list(package.schema),
            "call_count": call_count,
            "verified": package_ok and evidence_ok,
        }

    def _digest_evidence_from_store(self, entry_id: str) -> str:
        from .encoding import StreamHasher

        hasher = StreamHasher(namespace="observation-log-v1")
        for call in self._conn.execute(
            "SELECT seq, method, args_json, outcome, error_type, result_digest, "
            "detail_json FROM calls WHERE entry_id = ? ORDER BY seq",
            (entry_id,),
        ):
            hasher.update_encoded(canonical_json([
                call["seq"], call["method"], json.loads(call["args_json"]),
                call["outcome"], call["error_type"],
                call["result_digest"], json.loads(call["detail_json"]),
            ]))
        return hasher.hexdigest()

    def launches_for_job(self, job_version: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(SUM(launches), 0) FROM runs WHERE job_version = ?",
            (job_version,),
        ).fetchone()
        return int(row[0])

    def list_runs(self, job_version: Optional[str] = None) -> list[dict[str, Any]]:
        if job_version is None:
            rows = self._conn.execute("SELECT * FROM runs ORDER BY started_at").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM runs WHERE job_version = ? ORDER BY started_at",
                (job_version,),
            ).fetchall()
        return [dict(r) for r in rows]

    def violations(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM determinism_violations ORDER BY created_at"
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ runs

    def _start_run(
        self, run_id: str, identity: JobIdentity, snapshot: Snapshot, mode: str
    ) -> None:
        self._conn.execute(
            "INSERT INTO runs(run_id, job_version, snapshot_id, mode, started_at, "
            "status, launches) VALUES (?, ?, ?, ?, ?, ?, 0)",
            (run_id, identity.version, snapshot.snapshot_id, mode, time.time(), "running"),
        )
        self._conn.commit()

    def _finish_run(
        self,
        run_id: str,
        mode: str,
        status: str,
        reused_entry_id: Optional[str] = None,
        replay: Optional[ReplayReport] = None,
    ) -> None:
        self._conn.execute(
            "UPDATE runs SET mode = ?, status = ?, finished_at = ?, "
            "reused_entry_id = ?, replay_json = ? WHERE run_id = ?",
            (
                mode,
                status,
                time.time(),
                reused_entry_id,
                json.dumps(replay.as_dict()) if replay is not None else None,
                run_id,
            ),
        )
        self._conn.commit()

    def _fail_run(self, run_id: str, exc: Exception) -> None:
        self._conn.execute(
            "UPDATE runs SET mode = 'executed', status = 'failed', "
            "finished_at = ?, error = ? WHERE run_id = ?",
            (time.time(), "%s: %s" % (type(exc).__name__, exc), run_id),
        )
        self._conn.commit()
