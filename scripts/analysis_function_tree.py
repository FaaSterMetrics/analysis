#!/usr/bin/env python3
import json
import pathlib
from pprint import pprint
from typing import List

from collections import defaultdict, Counter

from argmagic import argmagic
import faastermetrics as fm
import faastermetrics.helper as fh
import faastermetrics.calls as fr


def print_request_tree(entries: List[fm.LogEntry]):
    """Print requests based on their xpairs and context ids."""
    group_data = fh.group_by(entries, lambda e: e.id)
    fh.print_group(group_data)

    rgs = fr.create_requestgroups(entries)
    for call in rgs:
        print(call)


def print_function_tree(entries: List[fm.LogEntry]):
    # group entries by function
    by_fn = defaultdict(list)
    for entry in entries:
        if "event" not in entry.data:
            continue
        by_fn[entry.fn["name"]].append(entry)

    for name, fn_entries in by_fn.items():
        subtypes = Counter([type(e) for e in fn_entries])
        print(f"{name}: {len(fn_entries)} entries : {subtypes}")
        if name == "undefined":
            continue
        # for entry in fn_entries:
        #     pprint(entry.data)


def main(logdump: pathlib.Path):
    """Print the function tree.

    Args:
        logdump: Path to log json dump.
    """

    data = fm.load_logs(logdump)

    print_function_tree(data)

    print_request_tree(data)


if __name__ == "__main__":
    argmagic(main, positional=("logdump",))
