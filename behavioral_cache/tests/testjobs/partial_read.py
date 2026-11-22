"""Test job: deliberately stops reading after the first row.

The recorded observation must capture exactly how far the job consumed the
stream (``truncated=True`` and one row); replay compares only that prefix.
"""

from bcache import OutputSink, RecordingReader


def run(reader: RecordingReader, sink: OutputSink, params) -> None:
    limit = int(params["limit"]) if params else 1
    seen = []
    for row in reader.read("nums", None, ["v"]):
        seen.append(row.values["v"])
        if len(seen) >= limit:
            break
    sink.declare([("first", "int64"), ("consumed", "int64")])
    sink.write_row({
        "first": seen[0] if seen else 0,
        "consumed": len(seen),
    })
