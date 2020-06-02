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
import faastermetrics.helper as fg
from faastermetrics.requestgroup import create_requestgroups


def get_runtime(perfs):
    try:
        measure, = filter(lambda e: e.perf["entryType"] == "measure", perfs)
        duration = measure.perf["duration"]
    except ValueError:
        duration = None
    return duration


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
                inner_times = [d for d in [
                    get_runtime(rg.get_rpc_in()) for rg in rgroups
                    if rg.context_id == rgroup.context_id and rg.function == function
                ] if d is not None]
                if graph.has_edge(rgroup.function, function):
                    graph.edges[rgroup.function, function]["outer_times"] += outer_times
                    graph.edges[rgroup.function, function]["inner_times"] += inner_times
                else:
                    graph.add_edge(rgroup.function, function, outer_times=outer_times, inner_times=inner_times)


    for edge in graph.edges:
        graph.edges[edge]["median_outer"] = np.round(np.median(graph.edges[edge]["outer_times"]) * 1, 2)
        graph.edges[edge]["median_inner"] = np.round(np.median(graph.edges[edge]["inner_times"]) * 1, 2)

    return graph


def set_graph_weight(graph, key):
    max_val = np.max(list(nx.get_edge_attributes(graph, key).values()))
    for edge in graph.edges:
        graph.edges[edge]["weight"] = max_val + 1 - graph.edges[edge][key]



def plot_graph(graph, plotdir, key="median_outer"):
    set_graph_weight(graph, key)
    pos = nx.random_layout(graph)
    pos = nx.spring_layout(graph, pos=pos, iterations=100, threshold=0.0, weight="weight", k=100/np.sqrt(len(graph.nodes)))
    # pos = nx.kamada_kawai_layout(graph, pos=pos, weight="weight")

    pos_higher = {}
    y_off = 0.1  # offset on the y axis
    for k, v in pos.items():
        pos_higher[k] = (v[0], v[1]+y_off)

    fig, ax = plt.subplots(figsize=(12, 8))
    nx.draw(graph, pos, edgecolors="black", node_color="white")
    nx.draw_networkx_labels(graph, pos_higher)
    nx.draw_networkx_edge_labels(graph, pos, edge_labels=nx.get_edge_attributes(graph, key))
    plt.savefig(plotdir / f"fgraph_{key}.png", dpi=300)
    plt.close()


def analyze_tree(data: List[fm.LogEntry], plotdir: pathlib.Path):
    """Build the call graph from the given logging data.
    """
    rgroups = create_requestgroups(data)
    graph = build_graph(rgroups)

    # plot_graph(graph, plotdir, key="transport_simple")
    # plot_graph(graph, plotdir, key="median_inner")
    plot_graph(graph, plotdir, key="median_outer")


def main(data: pathlib.Path, output: pathlib.Path):
    # data = pathlib.Path("output/logdumps/platform_aws.json")
    # output = pathlib.Path("output/plots/")

    output = output / data.stem
    output.mkdir(parents=True, exist_ok=True)

    data = fm.load_logs(data)
    analyze_tree(data, output)


if __name__ == "__main__":
    argmagic(main, positional=("data", "output"))
    # main("", "")
