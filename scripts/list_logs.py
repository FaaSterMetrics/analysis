#!/usr/bin/env python3
import json
import pathlib
from collections import Counter

from argmagic import argmagic

import faastermetrics as fm


def list_logs(logdir: pathlib.Path):
    """
    Simple script for some initial testing on logging data.

    Args:
        logdir: Directory containing logging data.
    """

    log_entries = [
        e
        for p in logdir.iterdir()
        for e in fm.parse_logfile(p)
    ]

    print("= Available logs =")
    platform_entries = Counter([e.platform for e in log_entries])
    for name, entries in platform_entries.items():
        print(f"{name}: {entries} entries")


if __name__ == "__main__":
    argmagic(list_logs, positional=("logdir",))
