"""Hand-written contract job 1: conditional rate lookup over regional sales.

Read behaviour (in order):

1. ``exists("rates")`` — the supplementary table may not be present.
2. When it exists, ``lookup("rates", "A")``; a missing key falls back to 10.
3. ``read("sales", region = "N", ["amount"])`` and sum the amounts.
4. Multiply by the rate and emit one row.

The job only ever sees data through the supplied reader; it has no imports
of os/time/random and cannot reach the network.
"""

from __future__ import annotations

from bcache import OutputSink, RecordingReader, eq

DEPENDENCIES: list[tuple[str, str]] = []


def run(reader: RecordingReader, sink: OutputSink, params) -> None:
    rate = 10
    if reader.exists("rates"):
        result = reader.lookup("rates", "A", key_col="code")
        if result.status == "found":
            rate = result.value["value"]
        # key_not_found / empty_table deliberately keep the default 10.

    total = 0
    for row in reader.read("sales", eq("region", "N"), ["amount"]):
        total += row.values["amount"]

    sink.declare([("total", "int64")])
    sink.write_row({"total": total * rate})
