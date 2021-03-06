"""
Generate a request graph based on a list of log entries.
"""
from collections import Counter

import numpy as np

import networkx as nx
from .logentry import LogEntry
from .calls import create_requestgroups
from .helper import uniq_by, group_by


def build_call_graph(entries: LogEntry) -> nx.DiGraph:
    calls = create_requestgroups(entries)

    count_ids = Counter([c.id for c in calls])
    if max(count_ids.values()) > 1:
        raise ValueError(f"Duplicate ids: { {k: v for k, v in count_ids.items() if v > 1} }")

    graph = nx.DiGraph()

    for call in calls:
        graph.add_node(call.id, calls=[call])
        # add edges
        for subcall in call.calls:
            matched_call, = [c for c in calls if c.id == subcall.id]
            graph.add_node(matched_call.id, calls=[matched_call])
            graph.add_edge(call.id, subcall.id, calls=[subcall])

    return graph


def build_function_graph(entries: LogEntry) -> nx.DiGraph:
    """Create a networkx graph that contains calls.

    Each edge contains outgoing calls that are logged externally (eg rpcOut)
    """
    calls = create_requestgroups(entries)

    graph = nx.DiGraph()

    for call in calls:
        # add edges
        for subcall in call.calls:
            subcall_function = subcall.function
            # subcall_function = id_names.get(subcall.id, subcall.function)

            if not graph.has_edge(call.function, subcall_function):
                graph.add_edge(call.function, subcall_function, calls=[subcall])
            else:
                graph.edges[(call.function, subcall_function)]["calls"].append(subcall)

            if "calls" not in graph.nodes[subcall_function]:
                graph.nodes[subcall_function]["calls"] = []

        if not graph.has_node(call.function):
            graph.add_node(call.function)

        # add nodes
        if "calls" not in graph.nodes[call.function]:
            graph.nodes[call.function]["calls"] = [call]
        else:
            graph.nodes[call.function]["calls"].append(call)

    return graph


def conv_to_ms(timedelta):
    return timedelta.total_seconds() * 1000


def node_num_calls(graph, node):
    calls = graph.nodes[node]["calls"]
    return len(calls)


def node_platform(graph, node):
    calls = graph.nodes[node]["calls"]
    platforms = [e.platform for c in calls for e in c.entries]
    uniq_platforms = set(platforms)
    if len(uniq_platforms) != 1:
        raise RuntimeError(f"{node} has wrong platform number: {uniq_platforms}")
    platform, = uniq_platforms
    return platform


def node_rpc_in_duration(graph, node, reduce_fun=np.mean):
    """Calculate rpcIn for the given node."""
    calls = graph.nodes[node]["calls"]
    durations = [conv_to_ms(c.duration) for c in calls if c.duration is not None]
    duration = reduce_fun(durations)
    return duration


def edge_rpc_out_duration(graph, edge, reduce_fun=np.mean):
    calls = graph.edges[edge]["calls"]
    durations = [conv_to_ms(c.duration) for c in calls if c.duration is not None]
    duration = reduce_fun(durations)
    return duration


def edge_transport_duration(graph, edge, reduce_fun=np.mean):
    _, dest = edge
    dest_calls = graph.nodes[dest]["calls"]
    edge_calls = graph.edges[edge]["calls"]

    transport_times = []
    edge_dest = [(e, d) for e in edge_calls for d in dest_calls if e.id == d.id]
    for e_call, d_call in edge_dest:
        duration = conv_to_ms(e_call.duration - d_call.duration)
        transport_times.append(duration)
    transport_time = reduce_fun(transport_times)
    return transport_time


def apply_to_graph_nodes(graph, fun, key):
    nx.set_node_attributes(graph, {n: fun(graph, n) for n in graph.nodes}, key)
    return graph


def apply_to_graph_edges(graph, fun, key):
    nx.set_edge_attributes(graph, {e: fun(graph, e) for e in graph.edges}, key)
    return graph


def add_default_metadata(graph):
    apply_to_graph_nodes(graph, node_rpc_in_duration, "rpc_in")
    apply_to_graph_nodes(graph, node_platform, "platform")

    apply_to_graph_edges(graph, edge_rpc_out_duration, "rpc_out")
    apply_to_graph_edges(graph, edge_transport_duration, "transport")
    return graph
