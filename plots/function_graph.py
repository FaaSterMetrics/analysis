#!/usr/bin/env python3
"""
Plot a graph representation of the logs.
"""
from typing import List
import pathlib
from argmagic import argmagic

import numpy as np

import networkx as nx

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import faastermetrics as fm
import faastermetrics.contextgroup as fg
from faastermetrics.perflog import create_requestgroups


def get_runtime(rgroup):
    incalls = rgroup.get_rpc_in()
    measure, = filter(lambda e: e.perf["entryType"] == "measure", incalls)
    return measure.perf["duration"]


def build_graph(rgroups):
    graph = nx.DiGraph()

    # add nodes
    for rgroup in rgroups:
        if rgroup.function not in graph.nodes:
            graph.add_node(rgroup.function, cids=[rgroup.context_id])
        else:
            graph.nodes[rgroup.function]["cids"].append(rgroup.context_id)

    # add edges
    for rgroup in rgroups:
        outentries = rgroup.get_rpc_out()
        if outentries:
            functions = fg.uniq_by(outentries, lambda e: e.perf["mark"].split(":")[-1])
            for function in functions:
                outer_times = [
                    e.perf["duration"] for e in outentries
                    if function in e.perf_name and e.perf["entryType"] == "measure"
                ]
                inner_times = [
                    get_runtime(rg) for rg in rgroups
                    if rg.context_id == rgroup.context_id and rg.function == function
                ]
                if graph.has_edge(rgroup.function, function):
                    graph.edges[rgroup.function, function]["outer_times"] += outer_times
                    graph.edges[rgroup.function, function]["inner_times"] += inner_times
                else:
                    graph.add_edge(rgroup.function, function, outer_times=outer_times, inner_times=inner_times)


    for edge in graph.edges:
        graph.edges[edge]["median_outer"] = np.round(np.median(graph.edges[edge]["outer_times"]) * 1, 2)
        graph.edges[edge]["median_inner"] = np.round(np.median(graph.edges[edge]["inner_times"]) * 1, 2)

    return graph


def analyze_tree(data: List[fm.LogEntry], plotdir: pathlib.Path):
    """Build the call graph from the given logging data.
    """
    rgroups = create_requestgroups(data)
    graph = build_graph(rgroups)

    pos = nx.spring_layout(graph, weight="median_outer", k=10/np.sqrt(len(graph.nodes)))

    pos_higher = {}
    y_off = 0.05  # offset on the y axis
    for k, v in pos.items():
        pos_higher[k] = (v[0], v[1]+y_off)

    plt.subplot(111, dpi=300)
    nx.draw(graph, pos, edgecolors="black", node_color="white")
    nx.draw_networkx_labels(graph, pos_higher)
    nx.draw_networkx_edge_labels(graph, pos, edge_labels=nx.get_edge_attributes(graph, "median_outer"))
    plt.savefig(plotdir / "fgraph_median_outer.png")


def main(data: pathlib.Path, output: pathlib.Path):
    data = pathlib.Path("output/logdumps/platform_aws.json")
    output = pathlib.Path("output/plots/")

    output = output / data.stem
    output.mkdir(parents=True, exist_ok=True)

    data = fm.load_logs(data)
    analyze_tree(data, output)


if __name__ == "__main__":
    # argmagic(main, positional=("data", "output"))
    main("", "")
