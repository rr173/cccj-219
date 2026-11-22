"""Distinct observable failure modes of the data reader."""

from __future__ import annotations


class ReaderError(Exception):
    """Base class for reader-side observable errors."""


class TableNotFound(ReaderError):
    """The named relation does not exist in the pinned snapshot."""

    def __init__(self, table: str):
        super().__init__("table not found: %s" % table)
        self.table = table


class UnknownColumn(ReaderError):
    """The relation exists but a requested column is not in its schema."""

    def __init__(self, table: str, column: str):
        super().__init__("unknown column %r in table %s" % (column, table))
        self.table = table
        self.column = column


class KeyNotFound(ReaderError):
    """The table exists and the key column is declared, but the key is absent."""

    def __init__(self, table: str, key: object):
        super().__init__("key %r not found in table %s" % (key, table))
        self.table = table
        self.key = key


class SchemaError(ValueError):
    """Invalid schema declaration while building a snapshot."""


class SnapshotConflict(RuntimeError):
    """A snapshot id is already published with different content."""
