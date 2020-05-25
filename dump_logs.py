#!/usr/bin/env python3
import json
import pathlib
from collections import Counter

from argmagic import argmagic

import faastermetrics as fm


def dump_logs(logdir: pathlib.Path, outdir: pathlib.Path):
    """Output logs to the given destination directory.

    Args:
        logdir: Directory containing raw log entries.
        outdir: Destination for outputting collected log json.
    """
    log_entries = [e for p in logdir.iterdir() for e in fm.parse_logfile(p)]
    if outdir.is_dir():
        outdir = outdir / f"{logdir.name}.json"
    with open(outdir, "w") as jsfile:
        json.dump(log_entries, jsfile)


if __name__ == "__main__":
    argmagic(dump_logs, positional=("logdir", "outdir"))
