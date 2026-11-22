"""Hand-written contract job 2: directory-driven partition aggregation.

Read behaviour (in order):

1. ``list_partitions("daily/")`` — the directory listing itself is observed.
2. For each returned partition name, read its ``amount`` rows and sum them.

Adding a matching *empty* partition changes the listing observation, so the
cache must be invalidated and the job really rerun even though the numeric
total does not change.
"""

from __future__ import annotations

from bcache import OutputSink, RecordingReader

DEPENDENCIES: list[tuple[str, str]] = []


def run(reader: RecordingReader, sink: OutputSink, params) -> None:
    partitions = reader.list_partitions("daily/")
    grand_total = 0
    seen = 0
    for name in partitions:
        seen += 1
        for row in reader.read(name, None, ["amount"]):
            grand_total += row.values["amount"]

    sink.declare([("total", "int64"), ("partitions", "int64")])
    sink.write_row({"total": grand_total, "partitions": seen})
