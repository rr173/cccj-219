"""Test job: bool/int distinction and projected column type drift."""

from bcache import OutputSink, RecordingReader, eq


def run(reader: RecordingReader, sink: OutputSink, params) -> None:
    flag_rows = list(reader.read("flags", eq("flag", True), ["flag", "v"]))
    int_rows = list(reader.read("flags", eq("v", 1), ["v"]))
    sink.declare([("flag_hits", "int64"), ("int_hits", "int64")])
    sink.write_row(
        {"flag_hits": len(flag_rows), "int_hits": len(int_rows)}
    )
