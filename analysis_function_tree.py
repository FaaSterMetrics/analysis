#!/usr/bin/env python3
import json
import pathlib
from pprint import pprint
from typing import List

from collections import defaultdict

from argmagic import argmagic
import faastermetrics as fm


def print_function_tree(entries: List[fm.LogEntry]):
    # group entries by function
    by_fn = defaultdict(list)
    for entry in entries:
        if "event" not in entry.data:
            continue
        by_fn[entry.event["fn"]].append(entry)

    for name, fn_entries in by_fn.items():
        print(f"{name}: {len(fn_entries)} entries")
        if name == "undefined":
            continue
        # for entry in fn_entries:
        #     pprint(entry.data)


def main(logdump: pathlib.Path):
    """Print the function tree.

    Args:
        logdump: Path to log json dump.
    """

    with open(logdump, "r") as jsfile:
        data = json.load(jsfile)

    print_function_tree(data)


if __name__ == "__main__":
    argmagic(main, positional=("logdump",))
