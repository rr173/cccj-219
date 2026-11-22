"""Test job: open/closed range boundary observation."""

from bcache import OutputSink, RecordingReader, and_, ge, lt


def run(reader: RecordingReader, sink: OutputSink, params) -> None:
    lo = params["lo"]
    hi = params["hi"]
    rows = list(
        reader.read("nums", and_(ge("v", lo), lt("v", hi)), ["v"])
    )
    sink.declare([("sum", "int64")])
    sink.write_row({"sum": sum(r.values["v"] for r in rows)})
