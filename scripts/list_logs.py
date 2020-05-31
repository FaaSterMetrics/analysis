#!/usr/bin/env python3
import json
import pathlib
from collections import Counter

from argmagic import argmagic

import faastermetrics as fm


def _lprint(*args, level=0, lead=">"):
    lindent = level * "  " + lead
    print(lindent, *args)


def _print_log_folder(logdir: pathlib.Path, level: int):
    """Print the given directory."""
    lprint = lambda m: _lprint(m, level=level, lead=" ")
    lprint_detail = lambda ms: list(map(lambda m: _lprint(m, level=level + 1, lead=" "), ms))

    _lprint(logdir.name, level=level)

    all_entries = fm.parse_logdir(logdir)
    lprint(f"Total entries: {len(all_entries)}")

    platform_entries = Counter([e.platform for e in all_entries])
    lprint("Platforms: ")
    lprint_detail(f"{k}: {v}" for k, v in platform_entries.items())
    version = Counter([e.data.get("version", "NA") for e in all_entries])
    lprint("Versions: ")
    lprint_detail(f"{k}: {v}" for k, v in version.items())
    dates = Counter([e.timestamp.date() for e in all_entries])
    lprint("Dates: ")
    lprint_detail(f"{k.isoformat()}: {v}" for k, v in dates.items())


def _walk_dirs(logdir: pathlib.Path, level: int = 0):
    """Recursively walk through directories until logdir is reached."""
    if fm.is_log_folder(logdir):
        _print_log_folder(logdir, level)
    elif logdir.is_dir():
        _lprint(logdir.name, level=level)
        for subdir in sorted(logdir.iterdir()):
            _walk_dirs(subdir, level + 1)
    else:
        _lprint(f"{logdir.name} ignored", level=level)


def list_logs(logdir: pathlib.Path):
    """
    Simple script for some initial testing on logging data.

    Args:
        logdir: Directory containing logging data.
    """
    print("= Available logs =")
    _walk_dirs(logdir)


if __name__ == "__main__":
    argmagic(list_logs, positional=("logdir",))
