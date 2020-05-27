import json
import pathlib
from typing import Union, Callable, List, Dict
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from argmagic import argmagic

import faastermetrics as fm

sns.set_style("whitegrid")


def group_by(data: List[fm.LogEntry], key: Callable) -> Dict[str, List[fm.LogEntry]]:
    """Group by the given key or callable."""
    grouped = defaultdict(list)
    for entry in data:
        grouped[key(entry)].append(entry)
    return grouped


def get_cgroup_measure(cgroup: fm.ContextGroup):
    measure_entries = [
        entry for entry in cgroup.entries
        if isinstance(entry, fm.PerfLog) and entry.perf["entryType"] == "measure"
    ]
    if len(measure_entries) == 1:
        measure = measure_entries[0]
        return measure.perf["duration"]
    return None


def plot_function_execution_time(data, plot_dir):
    fn_data = group_by(data, lambda e: e.event["fn"])
    function_durations = {}
    for function, events in fn_data.items():
        print(function)
        context_groups = fm.create_context_groups(events)
        durations = [get_cgroup_measure(cg) for cg in context_groups]
        function_durations[function] = [d for d in durations if d is not None]

    fig, ax = plt.subplots(1, 1, figsize=(8, 6), dpi=300)
    ax.boxplot(function_durations.values(), labels=function_durations.keys())
    ax.set_ylabel("Execution Time (ms)")
    ax.set_title("Comparison of total execution time in current deployment.")
    fig.tight_layout()
    fig.savefig(str(plot_dir / "boxplot.png"))


def main(input_data, plot_dir):
    # setup input and output paths
    # input_data = pathlib.Path("output/2020-05-27_10-41-14.json")
    # plot_dir = pathlib.Path("output/plots") / input_data.stem

    plot_dir = plot_dir / input_data.stem
    plot_dir.mkdir(exist_ok=True, parents=True)

    data = fm.load_logs(input_data)

    plot_function_execution_time(data, plot_dir)

if __name__ == "__main__":
    argmagic(main, positional=("input_data", "plot_dir"))
