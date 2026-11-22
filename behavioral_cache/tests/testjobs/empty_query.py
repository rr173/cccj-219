"""Test job: an empty query now gaining rows must invalidate."""

from bcache import OutputSink, RecordingReader, eq


def run(reader: RecordingReader, sink: OutputSink, params) -> None:
    rows = list(reader.read("events", eq("kind", "new"), ["id"]))
    sink.declare([("count", "int64")])
    sink.write_row({"count": len(rows)})
