#!/usr/bin/env python3
import json
import pathlib
from pprint import pprint
from typing import List

from collections import defaultdict, Counter

from argmagic import argmagic
import faastermetrics as fm
import faastermetrics.graph as fg


def print_walk_node(graph, node, level=0):
    calls = graph.nodes[node]["calls"]
    indent = "  " * level
    add_info = ""
    if level == 0:
        add_info += str({c.id[0] for c in calls})
    print(f"{indent}{node}: calls: {len(calls)} : {add_info}")
    for succ in graph.successors(node):
        print_walk_node(graph, succ, level+1)


def print_function_tree(entries: List[fm.LogEntry]):
    """Print requests based on their xpairs and context ids."""
    graph = fg.build_function_graph(entries)
    for node, deg in graph.in_degree:
        if deg == 0:
            print(f"= Calltree induced on: {node} =")
            print_walk_node(graph, node)


def main(logdump: pathlib.Path):
    """Print the function tree.

    Args:
        logdump: Path to log json dump.
    """

    data = fm.load_logs(logdump)

    print_function_tree(data)
    # print_request_tree(data)


if __name__ == "__main__":
    argmagic(main, positional=("logdump",))
