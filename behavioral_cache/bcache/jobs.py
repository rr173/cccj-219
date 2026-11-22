"""Cooperative job contract: loading, version pinning and execution.

A job is a Python module providing::

    DEPENDENCIES = [("name", "digest"), ...]   # optional, default empty
    RUNTIME_ID = "python-3.11-sqlite-..."       # optional explicit identifier

    def run(reader, sink, params): ...           # required entry point

The job version is fixed by *all* of: the module source digest, its declared
dependency digests, the canonicalised parameters and the runtime environment
identifier.  Changing any of them is a different job version and never
reuses cache.

The cooperative contract is trusted but constrained: jobs receive only the
reader and the output sink, so they cannot reach the filesystem, network,
clock or randomness through the framework.  This system does not claim to
safely execute malicious code; jobs are trusted cooperating modules.
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import platform
from dataclasses import dataclass
from typing import Any, Optional

from .encoding import canonical_json, digest_bytes
from .output import OutputSink
from .reader import RecordingReader


class JobError(Exception):
    """Invalid job module or a job that raised during execution."""


def default_runtime_id() -> str:
    import sqlite3

    return "python-%s;sqlite-%s;%s-%s" % (
        platform.python_version(),
        sqlite3.sqlite_version,
        platform.system().lower(),
        platform.machine().lower(),
    )


@dataclass(frozen=True)
class JobIdentity:
    module_path: str
    module_digest: str
    dependencies: tuple[tuple[str, str], ...]
    params: Any
    runtime_id: str
    version: str


def load_job_module(module_path: str):
    if not os.path.isfile(module_path):
        raise JobError("job module not found: %s" % module_path)
    with open(module_path, "rb") as fh:
        source = fh.read()
    module_digest = hashlib.blake2b(source, digest_size=32).hexdigest()
    real_path = os.path.realpath(module_path)
    module_name = "bcache_job_%s" % module_digest[:16]
    # Isolated loading: do not put the directory on sys.path globally and do
    # not pollute sys.modules beyond this handle.
    spec = importlib.util.spec_from_file_location(module_name, real_path)
    if spec is None or spec.loader is None:
        raise JobError("cannot create module spec for %s" % module_path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except Exception as exc:  # noqa: BLE001 - surface job import errors
        raise JobError("job module failed to import: %s" % exc) from exc
    if not callable(getattr(module, "run", None)):
        raise JobError("job module %s has no run(reader, sink, params) entry" % module_path)
    return module, module_digest


def _normalise_dependencies(module) -> tuple[tuple[str, str], ...]:
    raw = getattr(module, "DEPENDENCIES", [])
    deps = []
    for item in raw:
        if (
            not isinstance(item, (tuple, list))
            or len(item) != 2
            or not all(isinstance(x, str) for x in item)
        ):
            raise JobError("DEPENDENCIES entries must be (name, digest) string pairs")
        deps.append((item[0], item[1]))
    return tuple(deps)


def build_identity(
    module_path: str,
    params: Any = None,
    runtime_id: Optional[str] = None,
) -> JobIdentity:
    module, module_digest = load_job_module(module_path)
    deps = _normalise_dependencies(module)
    if runtime_id is None:
        runtime_id = getattr(module, "RUNTIME_ID", None) or default_runtime_id()
    parts = [
        b"job-version-v1\n",
        canonical_json({"module_digest": module_digest}),
        canonical_json({"dependencies": [list(d) for d in deps]}),
        canonical_json({"params": params}),
        canonical_json({"runtime_id": runtime_id}),
    ]
    version = digest_bytes(parts)
    return JobIdentity(
        os.path.realpath(module_path),
        module_digest,
        deps,
        params,
        runtime_id,
        version,
    )


def execute_job(
    module_path: str,
    recording_reader: RecordingReader,
    sink: OutputSink,
    params: Any,
) -> None:
    """Run the job entry point once, translating failures into JobError."""
    module, _ = load_job_module(module_path)
    try:
        module.run(recording_reader, sink, params)
    except JobError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise JobError("job raised %s: %s" % (type(exc).__name__, exc)) from exc
