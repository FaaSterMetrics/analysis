#!/usr/bin/env python3
import pathlib

from argmagic import argmagic

import faastermetrics as fm


def main(logdir: pathlib.Path):
    """
    Simple script for some initial testing on logging data.

    Args:
        logdir: Directory containing logging data.
    """

    files = {p.stem: fm.parse_logfile(p) for p in logdir.iterdir()}

    print("= Available logs =")
    for name, lines in files.items():
        print(f"{name}: {len(lines)} entries")


if __name__ == "__main__":
    argmagic(main, positional=("logdir",))
