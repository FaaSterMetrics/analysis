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


def classic_style(graph, *_, **__):

    def format_graph(graph):
        def format_node_labels(graph):
            labels = {}
            for fname, fdata in graph.nodes(data=True):
                fcalls = len(fdata["calls"])
                fduration = fdata["rpc_in"]
                if fduration is None:
                    label = f"{fname}\n({fcalls} calls)"
                else:
                    label = f"{fname}\n({fcalls} calls, {fduration:.2f}ms)"
                labels[fname] = label

            return labels

        def format_edge_labels(graph):
            labels = {}
            for (*edge, edata) in graph.edges(data=True):
                e_outer = edata.get("rpc_out", None)
                e_trans = edata.get("transport", None)

                labels[tuple(edge)] = f"Total: {e_outer:.2f}ms\nTransport: {e_trans:.2f}ms"

            return labels

        nx.set_node_attributes(graph, format_node_labels(graph), "label")
        nx.set_edge_attributes(graph, format_edge_labels(graph), "label")
        return graph

    graph = format_graph(graph)

    # add request nodes
    for node in list(graph.nodes.keys()):
        if len(graph.out_edges(node)) > 0 and len(graph.in_edges(node)) == 0:
            graph.add_node("__req_origin__", label="", height=0.2, width=0.2)
            graph.add_edge("__req_origin__", node)
    return graph


def classic_style_agraph(A, graph, cluster_key="platform"):
    A.graph_attr.update(rankdir="LR")

    node_vals = nx.get_node_attributes(graph, cluster_key)
    val_nodes = defaultdict(list)
    for node, value in node_vals.items():
        val_nodes[value].append(node)

    # create subgraphs
    for value, nodes in val_nodes.items():
        subgraph_name = f"cluster_{value}"
        A.add_subgraph(nodes, name=subgraph_name, label=value)

    A.layout("dot")
    return A


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


def format_graph_modern(graph, show_time=True):
    def format_node_label(node, data):
        if show_time:
            return f'<<table cellpadding="2" border="0" cellborder="0"><tr><td>{node}</td></tr><tr><td><FONT point-size="10">{data["rpc_in"]:.2f}ms</FONT></td></tr></table>>'
        else:
            return node

    node_labels = {
        n: format_node_label(n, data)
        for n, data in graph.nodes(data=True)
    }
    nx.set_node_attributes(graph, node_labels, "label")

    def pad_edge_label(label):
        if show_time:
            return f'<<table cellpadding="5" border="0" cellborder="0"><tr><td>{label}</td></tr></table>>'
        else:
            return ""

    edge_labels = {
        (a, b): pad_edge_label(f"{data['rpc_out']:.2f}ms")
        for (a, b, data) in graph.edges(data=True)
    }
    nx.set_edge_attributes(graph, edge_labels, "label")
    return graph


def set_node_attributes(graph, attributes):
    for key, value in attributes.items():
        nx.set_node_attributes(graph, value, key)


def set_edge_attributes(graph, attributes):
    for key, value in attributes.items():
        nx.set_edge_attributes(graph, value, key)


def modern_style(graph, show_time):
    graph = format_graph_modern(graph, show_time=show_time)
    colors = [
        "#2b2d42",  # bgdark
        "#8d99ae",  # dark
        "#edf2f4",  # bright
        "#ef233c",  # hlcolor
        "#d90429",  # hlcolor2
    ]
    # node formatting
    set_node_attributes(graph, {
        "shape": "box",
        "fontname": "Futura Bold",
        "fontsize": "18",
        "height": "1",
        "width": "3",
        "style": "filled",
        "penwidth": "10",
        "color": "#ffffff",
        "fillcolor": colors[0],
        "fontcolor": colors[2],
    })

    # edge formatting
    set_edge_attributes(graph, {
        "style": "dashed",
        "penwidth": 4,
        "arrowhead": "normal",
        "arrowsize": "1",
        "color": colors[1],
        "fontname": "Futura",
        "fontsize": "14",
        "fontcolor": colors[1],
    })
    graph.graph["overlap"] = "scale"
    graph.graph["esep"] = 3
    graph.graph["dpi"] = 300
    graph.graph["nodesep"] = 1
    graph.graph["ranksep"] = 1.1

    if graph.has_node("artillery"):
        graph.nodes["artillery"]["fillcolor"] = colors[1]
        graph.nodes["artillery"]["style"] = "filled"
        graph.nodes["artillery"]["fontcolor"] = colors[2]
        graph.nodes["artillery"]["penwidth"] = 10
        graph.nodes["artillery"]["height"] = 0.75
        graph.nodes["artillery"]["label"] = "artillery"  # do not include any weird time label in artillery

    return graph


def modern_style_agraph(A, graph):
    colors = [
        "#2b2d42",
        "#8d99ae",
        "#edf2f4",
        "#ef233c",
        "#d90429",
    ]

    node_vals = nx.get_node_attributes(graph, "platform")
    val_nodes = defaultdict(list)
    for node, value in node_vals.items():
        if value != "artillery":
            val_nodes[value].append(node)

    # create subgraphs
    for value, nodes in val_nodes.items():
        subgraph_name = f"cluster_{value}"
        A.add_subgraph(
            nodes, name=subgraph_name, label=value,
            labelloc="b", fontname="Futura Bold", fontsize="12",
            fontcolor=colors[0],
            fillcolor=colors[2], style="filled", penwidth=1, color=colors[0],
        )
        # set border color for included nodes
        for node in nodes:
            A.get_node(node).attr["color"] = colors[2]

    A.layout("dot")

    return A


STYLES = {
    "classic": (classic_style, classic_style_agraph),
    "modern": (modern_style, modern_style_agraph),
}


def plot_graph(graph, plotdir, style="classic", show_time=True):
    gfun, afun = STYLES[style]

    graph = gfun(graph, show_time)

    A = to_agraph(graph)

    A = afun(A, graph)
    A.draw(str(plotdir / "gviz_fgraph.png"), format="png")


def analyze_tree(data: List[fm.LogEntry], plotdir: pathlib.Path, style: str, functions: List[str], show_time: bool):
    """Build the call graph from the given logging data.
    """
    graph = build_function_graph(data)
    graph = add_default_metadata(graph)

    # generate subgraph based on given functions
    if functions:
        graph = graph.subgraph(functions)

    plot_graph(graph, plotdir, style=style, show_time=show_time)


def main(
        data: pathlib.Path,
        output: pathlib.Path,
        style: str = "classic",
        functions: List[str] = lambda: list(),
        notime: bool = False):
    """
    Args:
        data: Path to json log dump.
        output: Output graph folder.
        style: Set style of output graph.
        functions: Only show specific functions in graph. (eg "[frontend, add]")
        notime: Hide rpcIn and rpcOut times.
    """
    output = output / data.stem
    output.mkdir(parents=True, exist_ok=True)

    data = fm.load_logs(data)
    analyze_tree(data, output, style, functions, not notime)


if __name__ == "__main__":
    argmagic(main, positional=("data", "output"), use_flags=True)
