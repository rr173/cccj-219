"""Test job: observes a missing table (negative read), tolerates its absence."""

from bcache import OutputSink, RecordingReader


def run(reader: RecordingReader, sink: OutputSink, params) -> None:
    exists = reader.exists("optional_metrics")
    total = -1
    if exists:
        total = sum(r.values["m"] for r in reader.read("optional_metrics", None, ["m"]))
    sink.declare([("exists", "bool"), ("total", "int64")])
    sink.write_row({"exists": exists, "total": total})
