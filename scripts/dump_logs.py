#!/usr/bin/env python3
import json
import pathlib
from collections import Counter

from argmagic import argmagic

import faastermetrics as fm


def dump_logs(logdir: pathlib.Path, outdir: pathlib.Path, version: str = None):
    """Output logs to the given destination directory.

    Args:
        logdir: Directory containing raw log entries.
        outdir: Destination for outputting collected log json.
        version: Version requirement for inclusion.
    """
    log_entries = fm.parse_logdir(logdir)
    print(f"Loading {len(log_entries)} entries from {logdir}")

    if version is not None:
        print(f"Filtering: version={version}")
        log_entries = filter(lambda e: e.data["version"] == version, log_entries)

    log_entries = list(log_entries)


    if outdir.is_dir():
        outdir = outdir / f"{logdir.name}.json"

    print(f"Dumping {len(log_entries)} entries to {outdir}")
    with open(outdir, "w") as jsfile:
        json.dump(log_entries, jsfile)


if __name__ == "__main__":
    argmagic(dump_logs, positional=("logdir", "outdir"))
