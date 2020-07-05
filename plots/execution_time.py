import pathlib
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from argmagic import argmagic

import faastermetrics as fm
from faastermetrics.helper import group_by
from faastermetrics.calls import create_requestgroups

sns.set_style("whitegrid")


def timedelta_to_ms(timedelta):
    return timedelta.total_seconds() * 1000


def plot_boxplot(data, ylabel, title, log_scaling=True):
    """
    Args:
        data: Dict of list of numbers.
    """

    fig, ax = plt.subplots(1, 1, figsize=(8, 6), dpi=300)
    plt.xticks(rotation=90)
    ax.boxplot(data.values(), labels=data.keys())
    ax.set_ylabel(ylabel)
    ax.set_title(title)

    if log_scaling:
        ax.set_yscale("log")

    fig.tight_layout()

    return fig


def plot_platform_transport_times(data, plot_dir):
    """Get the average transport time between different platforms."""
    cgroups = create_requestgroups(data)

    # get function platform associations
    platforms = {
        call.function: entry.platform for call in cgroups for entry in call.entries
    }

    transport_times = defaultdict(list)
    # get transport times
    for call in cgroups:
        if call.function == "artillery":
            continue
        for subcall in call.calls:
            try:
                matched_call, = [c for c in cgroups if c.id == subcall.id]
            except ValueError:
                continue

            orig = platforms[call.function]
            dest = platforms[subcall.function]
            transport = (subcall.duration - matched_call.duration) / 2
            transport_times[(orig, dest)].append(timedelta_to_ms(transport))

    fig = plot_boxplot(
        transport_times,
        ylabel="Transport Time (ms)",
        title="Network traffic time between platforms.",
        log_scaling=True
    )

    plot_path = str(plot_dir / "boxplot_network_transport.png")
    print(f"Plotting to {plot_path}")
    fig.savefig(plot_path)

def plot_function_execution_time_frontend(data, plot_dir):
    cgroups = create_requestgroups(data)
    fn_calls = group_by(cgroups, lambda c: c.function)
    function_durations = {}
    for function, calls in fn_calls.items():
        if function is not None and "frontend" in function:
            function_durations[function] = [
                timedelta_to_ms(c.duration) for c in calls if c.duration is not None
            ]

    fig = plot_boxplot(
        function_durations,
        ylabel="Execution Time (ms)",
        title="Comparison of total execution time for frontend functions.",
        log_scaling=True
    )

    plot_path = str(plot_dir / "boxplot_frontend_function_execution_time.png")
    print(f"Plotting to {plot_path}")
    fig.savefig(plot_path)


def plot_function_execution_time(data, plot_dir):
    cgroups = create_requestgroups(data)
    fn_calls = group_by(cgroups, lambda c: c.function)
    function_durations = {}
    for function, calls in fn_calls.items():
        if function not in ("artillery", None):
            function_durations[function] = [
                timedelta_to_ms(c.duration) for c in calls if c.duration is not None
            ]

    fig = plot_boxplot(
        function_durations,
        ylabel="Execution Time (ms)",
        title="Comparison of total execution time in current deployment.",
        log_scaling=True
    )

    plot_path = str(plot_dir / "boxplot_function_execution_time.png")
    print(f"Plotting to {plot_path}")
    fig.savefig(plot_path)


def main(input_data: pathlib.Path, plot_dir: pathlib.Path):
    # plot_dir = plot_dir / input_data.stem
    plot_dir.mkdir(exist_ok=True, parents=True)

    data = fm.load_logs(input_data)

    plot_function_execution_time(data, plot_dir)
    plot_function_execution_time_frontend(data, plot_dir)
    plot_platform_transport_times(data, plot_dir)

if __name__ == "__main__":
    argmagic(main, positional=("input_data", "plot_dir"))
