"""Test job: raises at runtime; failed runs must never become cache."""


def run(reader, sink, params):
    reader.exists("anything")
    raise RuntimeError("boom: job failed on purpose")
