#!/usr/bin/env python3
"""Top-level entry point so the CLI works under ``python3 -I``.

Examples::

    python3 -I bcache_run.py run jobs/rate_sales.py --registry ... --cache ...
    python3 -I bcache_run.py inspect <entry_id> --cache ...
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bcache.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
