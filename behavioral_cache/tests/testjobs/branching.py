"""Test job: dynamic branch driven by an earlier read result.

The first read selects "mode" rows; the mode decides which table is read
next, so a change in the first observation can take a completely different
read path on replay.
"""

from bcache import OutputSink, RecordingReader, eq

DEPENDENCIES: list[tuple[str, str]] = []


def run(reader: RecordingReader, sink: OutputSink, params) -> None:
    branch = "a"
    if reader.exists("config"):
        rows = list(reader.read("config", eq("k", "mode"), ["v"]))
        if rows and rows[0].values["v"] == "b":
            branch = "b"
    table = "branch_a" if branch == "a" else "branch_b"
    total = 0
    if reader.exists(table):
        for row in reader.read(table, None, ["n"]):
            total += row.values["n"]
    sink.declare([("branch", "text"), ("total", "int64")])
    sink.write_row({"branch": branch, "total": total})
