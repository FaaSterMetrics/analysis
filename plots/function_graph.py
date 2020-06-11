#!/usr/bin/env python3
"""
Plot a graph representation of the logs.
"""
from typing import List
import pathlib
from collections import defaultdict, Counter

from argmagic import argmagic
import numpy as np

import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout, to_agraph
import pygraphviz as pgv

import faastermetrics as fm
import faastermetrics.helper as fg
from faastermetrics.calls import create_requestgroups


def get_rpc_in_duration(rgroups):
    """Calculate request times based on rpcIn."""
    all_durations = defaultdict(list)
    for rgroup in rgroups:
        # find outgoing calls
        duration = rgroup.duration
        if duration is not None:
            duration_ms = duration.total_seconds() * 1000
            all_durations[rgroup.function].append(duration_ms)
    return all_durations


def reduce_dict(data, fun):
    return {k: fun(v) for k, v in data.items()}


def get_num_calls(rgroups):
    return Counter([rgroup.function for rgroup in rgroups])


def get_rpc_out_duration(rgroups):
    """Calculate durations based on rpcOut."""
    times = defaultdict(list)
    for rgroup in rgroups:
        # iterate over outer duration measurements
        for r_out in rgroup.calls:
            duration_ms = r_out.duration.total_seconds() * 1000
            times[(rgroup.function, r_out.function)].append(duration_ms)

    return times


def get_transport_times(rgroups):
    times = defaultdict(list)
    for rgroup in rgroups:
        # iterate over outer duration measurements
        for entry in rgroup.calls:
            matched = [
                rg for rg in rgroups if rg.id == entry.id and rg.function == entry.function
            ]
            if len(matched) != 1:
                raise ValueError(f"{entry} has no matched call entry.")

            inner_duration = matched[0].duration
            outer_duration = entry.duration
            transport = (outer_duration - inner_duration)
            transport_ms = transport.total_seconds() * 1000

            times[(rgroup.function, entry.function)].append(transport_ms)

    return times


def get_rpc_in(rgroups, reduce=np.median):
    durations = get_rpc_in_duration(rgroups)
    return reduce_dict(durations, reduce)


def get_rpc_out(rgroups, reduce=np.median):
    outer = get_rpc_out_duration(rgroups)
    return reduce_dict(outer, reduce)


def get_transport(rgroups, reduce=np.median):
    transport = get_transport_times(rgroups)
    return reduce_dict(transport, reduce)


def get_platform(rgroups):
    entry_platforms = defaultdict(list)
    for rgroup in rgroups:
        entry_platforms[rgroup.function] += [e.platform for e in rgroup.entries]

    platforms = {}
    for fun, fplatforms in entry_platforms.items():
        uniq_platforms = set(fplatforms)
        if len(uniq_platforms) != 1:
            for rgroup in [r for r in rgroups if r.function == fun]:
                print(rgroup.entries[0].timestamp, rgroup.entries[0].platform)
            raise RuntimeError(f"{fun} has wrong platform number: {uniq_platforms}")
        platforms[fun] = next(iter(uniq_platforms))
    return platforms


def format_node_labels(graph):
    durations = nx.get_node_attributes(graph, "duration")
    calls = nx.get_node_attributes(graph, "calls")

    labels = {}
    for fname in calls:
        fcalls = calls.get(fname, 0)
        fduration = durations.get(fname, None)
        if fduration is None:
            label = f"{fname}\n({fcalls} calls)"
        else:
            label = f"{fname}\n({fcalls} calls, {fduration:.2f}ms)"
        labels[fname] = label

    return labels


def format_edge_labels(graph):
    outer = nx.get_edge_attributes(graph, "outer")
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


def build_graph(rgroups):
    graph = nx.DiGraph()

    # add nodes
    graph.add_nodes_from([rg.function for rg in rgroups])
    graph.add_edges_from([
        (rg.function, ro.function) for rg in rgroups for ro in rg.calls
    ])

    # nx.set_node_attributes(graph, "bold", "style")
    nx.set_node_attributes(graph, get_rpc_in(rgroups, np.mean), "duration")
    nx.set_node_attributes(graph, get_num_calls(rgroups), "calls")
    nx.set_node_attributes(graph, get_platform(rgroups), "platform")
    nx.set_node_attributes(graph, format_node_labels(graph), "label")
    # nx.set_node_attributes(graph, format_node_color(graph), "color")

    nx.set_edge_attributes(graph, get_rpc_out(rgroups, np.mean), "outer")
    nx.set_edge_attributes(graph, get_transport(rgroups, np.mean), "transport")

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
    rgroups = [rg for rg in create_requestgroups(data) if rg.id[0] is not None]
    graph = build_graph(rgroups)

    plot_graph(graph, plotdir, key="median_outer")


def main(data: pathlib.Path, output: pathlib.Path):
    output = output / data.stem
    output.mkdir(parents=True, exist_ok=True)

    data = fm.load_logs(data)
    analyze_tree(data, output)


if __name__ == "__main__":
    argmagic(main, positional=("data", "output"))
