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
from faastermetrics.helper import group_by, uniq_by
from faastermetrics.logentry import UNDEFINED_XPAIR
from faastermetrics.graph import build_function_graph, add_default_metadata, build_call_graph


STYLE_CLASSIC = {
    "node_style": {
    },
    "edge_style": {
    },
    "caller_style": {
        "height": 0.2,
        "width": 0.2,
    },
    "node_label_table": {
        "cellpadding": 2, "border": 0, "cellborder": 0
    },
    "node_label_table_font_small": {
        "point-size": 10,
    },
    "edge_label_table": {
        "cellpadding": 5, "border": 0, "cellborder": 0
    },
    "cluster_style": {
    },
    "graph": {
        "rankdir": "LR",
    },
    "layout": "dot",
}

MODERN_COLORS = [
    "#2b2d42",  # bgdark
    "#8d99ae",  # dark
    "#edf2f4",  # bright
    "#ef233c",  # hlcolor
    "#d90429",  # hlcolor2
]

STYLE_MODERN = {
    "node_style": {
        "shape": "box",
        "fontname": "Futura Bold",
        "fontsize": "18",
        "height": "1",
        "width": "3",
        "style": "filled",
        "penwidth": "10",
        "color": "#ffffff",
        "fillcolor": MODERN_COLORS[0],
        "fontcolor": MODERN_COLORS[2],
    },
    "edge_style": {
        "style": "dashed",
        "penwidth": 4,
        "arrowhead": "normal",
        "arrowsize": "1",
        "color": MODERN_COLORS[1],
        "fontname": "Futura",
        "fontsize": "14",
        "fontcolor": MODERN_COLORS[1],
    },
    "caller_style": {
        "fillcolor": MODERN_COLORS[1],
        "style": "filled",
        "fontcolor": MODERN_COLORS[2],
        "penwidth": 10,
        "height": 1,
    },
    "node_label_table": {
        "cellpadding": 2, "border": 0, "cellborder": 0
    },
    "node_label_table_font_small": {
        "point-size": 10,
    },
    "edge_label_table": {
        "cellpadding": 5, "border": 0, "cellborder": 0
    },
    "cluster_style": {
        "labelloc": "b",
        "fontname": "Futura Bold",
        "fontsize": "12",
        "fontcolor": MODERN_COLORS[0],
        "fillcolor": MODERN_COLORS[2],
        "style": "filled",
        "penwidth": 1,
        "color": MODERN_COLORS[0],
    },
    "graph": {
        "overlap": "scale",
        "dpi": 300,
        "nodesep": 1,
        "ranksep": 1.1,
    },
    "layout": "dot",
}


def set_single_node_attributes(graph, node, attributes):
    for key, item in attributes.items():
        graph.nodes[node][key] = item


def set_single_edge_attributes(graph, edge, attributes):
    for key, item in attributes.items():
        graph.edges[edge][key] = item


def set_node_attributes(graph, attributes):
    for key, value in attributes.items():
        nx.set_node_attributes(graph, value, key)


def set_edge_attributes(graph, attributes):
    for key, value in attributes.items():
        nx.set_edge_attributes(graph, value, key)


def get_caller_nodes(graph):
    callers = [n for n in graph.nodes if graph.nodes[n].get("iscaller", False)]
    return callers


def is_artillery(graph, node):
    return all(c.function == "artillery" for c in graph.nodes[node]["calls"])

def get_artillery_nodes(graph):
    artillery_nodes = [n for n in graph.nodes if is_artillery(graph, n)]
    return artillery_nodes


def get_node_function(graph, node):
    calls = graph.nodes[node]["calls"]
    function, = uniq_by(calls, lambda c: c.function)
    return function


def get_node_xpairs(graph, node):
    xpairs = {c.id[1] for c in graph.nodes[node].get("calls", [])}
    return xpairs


def format_attrs(attributes):
    attrs = " ".join(f'{k}="{v}"' for k, v in attributes.items())
    return attrs


def html_font(data, attributes):
    return f"<font {format_attrs(attributes)}>{data}</font>"


def html_table(rowcol_data, attributes):
    def destruct_item(item):
        if isinstance(item, tuple):
            d, attrs = item
        else:
            d, attrs = item, {}
        return d, attrs

    rows = ["".join(f"<td {format_attrs(d)}>{c}</td>" for c, d in [destruct_item(d) for d in r]) for r in rowcol_data]
    table_str = "".join(f"<tr>{row_str}</tr>" for row_str in rows)
    return f"<table {format_attrs(attributes)}>{table_str}</table>"


def format_graph(graph, filters, style):
    show_time = filters["show_time"]
    context_id = bool(filters["context_id"])

    def format_node_label(node, data):
        main_data = node
        sub_datas = []

        if context_id:
            main_data = node[1]

        if show_time:
            sub_datas += [f"{data['rpc_in']:.2f}ms"]

        if len(sub_datas) > 0:
            sub_datas = list(map(lambda s: html_font(s, style["node_label_table_font_small"]), sub_datas))
            sub_data = "".join(f"<td>{s}</td>" for s in sub_datas)
            label_str = html_table(
                [[(main_data, {"colspan": len(sub_datas)})], sub_datas],
                style["node_label_table"],
            )
            return f'<{label_str}>'
        else:
            return main_data

    node_labels = {
        n: format_node_label(n, data)
        for n, data in graph.nodes(data=True)
    }
    nx.set_node_attributes(graph, node_labels, "label")

    def format_edge_label(edge, data):
        labels = []
        if context_id:
            labels += [f"{data['calls'][0].id[1]}"]
        if show_time:
            labels += [f"{data['rpc_out']:.2f}ms"]

        # show no edge label until we figure out rpcOut issues: https://github.com/FaaSterMetrics/analysis/issues/24
        if context_id:
            return ""

        if len(labels) > 0:
            label = html_table([[l] for l in labels], style["edge_label_table"])
            return f'<{label}>'
        else:
            return ""

    edge_labels = {
        (a, b): format_edge_label((a, b), data)
        for (a, b, data) in graph.edges(data=True)
    }
    nx.set_edge_attributes(graph, edge_labels, "label")
    return graph


CALLER_NAMES = {
    "__caller__": "External Caller",
    "__artillery__": "Artillery Call",
}


def apply_graph_style(graph, filters, style):
    graph = format_graph(graph, filters, style)

    artillery_nodes = get_artillery_nodes(graph)
    if len(artillery_nodes) > 0:
        for node in artillery_nodes:
            graph.nodes[node]["iscaller"] = True
            graph.nodes[node]["calltype"] = "__artillery__"
    else:
        graph.add_node("__caller__", iscaller=True, calltype="__caller__")

        # connect node to nodes without xpair
        for node in graph:
            if UNDEFINED_XPAIR in get_node_xpairs(graph, node):
                graph.add_edge("__caller__", node, calls=[])

    # node formatting
    set_node_attributes(graph, style["node_style"])

    # edge formatting
    set_edge_attributes(graph, style["edge_style"])
    for key, item in style["graph"].items():
        graph.graph[key] = item

    for caller in get_caller_nodes(graph):
        set_single_node_attributes(graph, caller, style["caller_style"])

        # graph.nodes["artillery"]["label"] = "artillery"  # do not include any weird time label in artillery
        caller_name = CALLER_NAMES.get(graph.nodes[caller]["calltype"], "Unknown caller")
        if filters["context_id"]:
            table_data = [
                [f'Context ID {filters["context_id"]}'],
                [html_font(caller_name, style["node_label_table_font_small"])],
            ]
            if graph.nodes[caller]["calltype"] == "__artillery__":
                table_data.append([html_font(f"{graph.nodes[caller]['rpc_in']:.2f}ms", style["node_label_table_font_small"])])
            graph.nodes[caller]["label"] = "<" + html_table(table_data, style["node_label_table"]) + ">"
        else:
            graph.nodes[caller]["label"] = caller_name
    return graph


def apply_agraph_style(A, graph, filters, style):
    cluster_style = style["cluster_style"]

    node_vals = nx.get_node_attributes(graph, "platform")
    val_nodes = defaultdict(list)
    for node, value in node_vals.items():
        if value not in ("artillery", "__caller__"):
            val_nodes[value].append(node)

    # create subgraphs
    for value, nodes in val_nodes.items():
        subgraph_name = f"cluster_{value}"
        platgroup = A.add_subgraph(nodes, name=subgraph_name, label=value, **cluster_style)

        # set border color for included nodes
        for node in nodes:
            A.get_node(node).attr["color"] = cluster_style.get("fillcolor", "")

        # create nested subgraph on function if call graph
        if filters["context_id"]:
            for name, fnodes in group_by(nodes, lambda e: get_node_function(graph, e)).items():
                platgroup.add_subgraph(
                    fnodes, name=f"cluster_{name}", label=name,
                    **cluster_style
                )

    A.layout(style["layout"])

    return A


STYLES = {
    "classic": STYLE_CLASSIC,
    "modern": STYLE_MODERN,
}


def plot_graph(graph, plotdir, filters, style="classic"):
    style = STYLES[style]

    graph = apply_graph_style(graph, filters, style)

    A = to_agraph(graph)

    A = apply_agraph_style(A, graph, filters, style)
    A.draw(str(plotdir / "gviz_fgraph.png"), format="png")


def analyze_tree(data: List[fm.LogEntry], plotdir: pathlib.Path, style: str, filters: dict):
    """Build the call graph from the given logging data.
    """
    context_id = filters["context_id"]
    if context_id:
        len_before = len(data)
        data = [d for d in data if d.context_id == context_id]
        print(f"Filter on contextId({context_id}): {len(data)}/{len_before} included")
        graph = build_call_graph(data)
    else:
        graph = build_function_graph(data)

    graph = add_default_metadata(graph)

    tree_root = filters["function_tree"]
    if tree_root:
        succs = list(nx.dfs_preorder_nodes(graph, tree_root))
        preds = list(nx.dfs_preorder_nodes(graph.reverse(), tree_root))
        included = succs + preds
        graph = graph.subgraph(included)
        print(f"Only include nodes with connection to {tree_root}: {set(included)}")

    min_degree = filters["min_degree"]
    if min_degree:
        print(f"Removing nodes with degree lower equal {min_degree}")
        graph = graph.subgraph([n for n, d in graph.degree if d > min_degree])

    functions = filters["functions_only"]
    if functions:
        print(f"Only show: {functions}")
        graph = graph.subgraph(functions)

    plot_graph(graph, plotdir, filters, style=style)


def main(
        data: pathlib.Path,
        output: pathlib.Path,
        style: str = "classic",
        ftree: str = None,
        context: str = None,
        degree: int = 0,
        xpair: bool = False,
        functions: List[str] = lambda: list(),
        notime: bool = False):
    """
    Args:
        data: Path to json log dump.
        output: Output graph folder.
        style: Set style of output graph.
        functions: Only show specific functions in graph. (eg "[frontend, add]")
        degree: Minimal node degree filter.
        context: Filter on context id.
        ftree: Only show functions that are connected with the given function.
        notime: Hide rpcIn and rpcOut times.
        xpair: Show separate xpairs in individual nodes.
    """
    output = output / data.stem
    output.mkdir(parents=True, exist_ok=True)

    data = fm.load_logs(data)
    graph_filters = {
        "function_tree": ftree,
        "functions_only": functions,
        "context_id": context,
        "min_degree": degree,
        "xpair": xpair,
        "show_time": not notime,
    }
    analyze_tree(data, output, style, graph_filters)


if __name__ == "__main__":
    argmagic(main, positional=("data", "output"), use_flags=True)
