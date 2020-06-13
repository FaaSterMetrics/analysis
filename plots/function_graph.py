#!/usr/bin/env python3
"""
Plot a graph representation of the logs.
"""
from typing import List
import pathlib
from collections import defaultdict

from argmagic import argmagic
import numpy as np

import networkx as nx
from networkx.drawing.nx_agraph import to_agraph

import faastermetrics as fm
from faastermetrics.graph import build_function_graph, add_default_metadata


def format_node_labels(graph):
    durations = nx.get_node_attributes(graph, "rpc_in")
    calls = nx.get_node_attributes(graph, "calls")

    labels = {}
    for fname in calls:
        fcalls = len(calls.get(fname, []))
        fduration = durations.get(fname, None)
        if fduration is None:
            label = f"{fname}\n({fcalls} calls)"
        else:
            label = f"{fname}\n({fcalls} calls, {fduration:.2f}ms)"
        labels[fname] = label

    return labels


def format_edge_labels(graph):
    outer = nx.get_edge_attributes(graph, "rpc_out")
    trans = nx.get_edge_attributes(graph, "transport")

    labels = {}
    for edge in graph.edges:
        e_outer = outer.get(edge, None)
        e_trans = trans.get(edge, None)

        labels[edge] = f"Total: {e_outer:.2f}ms\nTransport: {e_trans:.2f}ms"

    return labels


def format_node_color(graph):
    platform = nx.get_node_attributes(graph, "platform")

    platform_colors = {
        "aws": "#eb7720",
        "google": "#e34335",
        "azure": "#0090C2",
    }

    node_fillcolor = {}
    for node in graph.nodes:
        node_fillcolor[node] = platform_colors[platform[node]]

    return node_fillcolor


def format_graph(graph):
    # nx.set_node_attributes(graph, "bold", "style")
    nx.set_node_attributes(graph, format_node_labels(graph), "label")
    # nx.set_node_attributes(graph, format_node_color(graph), "color")

    nx.set_edge_attributes(graph, format_edge_labels(graph), "label")

    return graph


def set_graph_weight(graph, key, dest_key="weight"):
    max_val = np.max(list(nx.get_edge_attributes(graph, key).values()))
    for edge in graph.edges:
        graph.edges[edge][dest_key] = max_val + 1 - graph.edges[edge][key]


def cluster_graph_on(A, node_vals):
    val_nodes = defaultdict(list)
    for node, value in node_vals.items():
        val_nodes[value].append(node)

    for value, nodes in val_nodes.items():
        subgraph_name = f"cluster_{value}"
        A.add_subgraph(nodes, name=subgraph_name, label=value)


def plot_graph(graph, plotdir, key="median_outer", cluster_key="platform"):
    graph = format_graph(graph)
    # add request nodes
    for node in list(graph.nodes.keys()):
        if len(graph.out_edges(node)) > 0 and len(graph.in_edges(node)) == 0:
            graph.add_node("__req_origin__", label="", height=0.2, width=0.2)
            graph.add_edge("__req_origin__", node)

    A = to_agraph(graph)
    A.graph_attr.update(rankdir="LR")

    if cluster_key is not None:
        cluster_graph_on(A, nx.get_node_attributes(graph, cluster_key))

    A.layout("dot")
    A.draw(str(plotdir / f"gviz_fgraph_{key}.png"))


def analyze_tree(data: List[fm.LogEntry], plotdir: pathlib.Path):
    """Build the call graph from the given logging data.
    """
    graph = build_function_graph(data)
    graph = add_default_metadata(graph)

    plot_graph(graph, plotdir, key="median_outer")


def main(data: pathlib.Path, output: pathlib.Path):
    output = output / data.stem
    output.mkdir(parents=True, exist_ok=True)

    data = fm.load_logs(data)
    analyze_tree(data, output)


if __name__ == "__main__":
    argmagic(main, positional=("data", "output"))
