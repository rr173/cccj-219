"""Offline processing runner with read-behaviour-validated caching."""

from .encoding import MISSING
from .errors import KeyNotFound, ReaderError, TableNotFound, UnknownColumn
from .jobs import JobIdentity, build_identity
from .materialize import (
    CorrectionLogError,
    KeyConflictError,
    MaterializeError,
    MaterializeResult,
    materialize_as_of,
)
from .output import OutputPackage, OutputSink, read_package
from .predicate import (
    and_,
    eq,
    ge,
    gt,
    is_null,
    le,
    lt,
    ne,
    not_,
    not_null,
    or_,
)
from .reader import LookupResult, Reader, RecordingReader, Row
from .reconcile import (
    PendingDecisions,
    ReconcileDecisionError,
    ReconcileError,
    ReconcileInputError,
    ReconcileResult,
    reconcile_table,
)
from .replayer import ReplayReport, ReplayValidator, observations_equal
from .rollup import (
    RollupContractError,
    RollupError,
    RollupOverflowError,
    RollupResult,
    RollupRevisionError,
    advance_sales_rollup,
)
from .snapshot import Snapshot, SnapshotBuilder, SnapshotRegistry
from .store import CacheStore, DeterminismViolation, RunResult, WhyReport

__all__ = [
    "MISSING",
    "KeyNotFound",
    "ReaderError",
    "TableNotFound",
    "UnknownColumn",
    "JobIdentity",
    "build_identity",
    "CorrectionLogError",
    "KeyConflictError",
    "MaterializeError",
    "MaterializeResult",
    "materialize_as_of",
    "OutputPackage",
    "OutputSink",
    "read_package",
    "and_",
    "eq",
    "ge",
    "gt",
    "is_null",
    "le",
    "lt",
    "ne",
    "not_",
    "not_null",
    "or_",
    "LookupResult",
    "Reader",
    "RecordingReader",
    "Row",
    "PendingDecisions",
    "ReconcileDecisionError",
    "ReconcileError",
    "ReconcileInputError",
    "ReconcileResult",
    "reconcile_table",
    "ReplayReport",
    "ReplayValidator",
    "observations_equal",
    "RollupContractError",
    "RollupError",
    "RollupOverflowError",
    "RollupResult",
    "RollupRevisionError",
    "advance_sales_rollup",
    "Snapshot",
    "SnapshotBuilder",
    "SnapshotRegistry",
    "CacheStore",
    "DeterminismViolation",
    "RunResult",
    "WhyReport",
]
