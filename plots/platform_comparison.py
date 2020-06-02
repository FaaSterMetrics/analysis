import pathlib
from collections import defaultdict

import numpy as np

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import seaborn as sns

from argmagic import argmagic

import faastermetrics as fm


sns.set_style("whitegrid")


def get_function_measures(entries):
    """Return a dict with lists of rpcIn measures."""
    measures = defaultdict(list)
    for entry in entries:
        if hasattr(entry, "perf") and entry.perf["entryType"] == "measure":
            measures[entry.fn["name"]].append(entry.perf["duration"])
    return measures


def get_average_duration(measures):
    return {k: np.mean(v) for k, v in measures.items()}


def logs_to_long_form(log_entries):
    long_data = defaultdict(list)
    for platform, entries in log_entries.items():
        for function, durations in get_function_measures(entries).items():
            for duration in durations:
                long_data["function"].append(function)
                long_data["platform"].append(platform)
                long_data["duration"].append(duration)
    return long_data


def plot_platform_comparison(all_logs, plot_dir):
    plot_path = plot_dir / f"platform_comparison.png"

    data = logs_to_long_form(all_logs)

    x = data["function"]
    y = data["duration"]
    hue = data["platform"]

    fig, ax = plt.subplots(figsize=(16, 12))
    sns.boxplot(x=x, y=y, hue=hue, palette=["m", "g", "r"], ax=ax)
    ax.set(yscale="log")
    ax.set_title("Comparison of function run duration on different FaaS platforms (single provider deployment)")
    fig.tight_layout()
    plt.savefig(str(plot_path), dpi=300)
    plt.close()


def main(logpath: pathlib.Path, output: pathlib.Path):
    output = output / logpath.name
    output.mkdir(exist_ok=True, parents=True)

    platform_logs = {f.stem.split("_")[-1]: fm.load_logs(f) for f in logpath.glob("*.json")}

    plot_platform_comparison(platform_logs, output)


if __name__ == "__main__":
    argmagic(main, positional=("logpath", "output"))
