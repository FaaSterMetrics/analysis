import pathlib
from collections import defaultdict
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from argmagic import argmagic

import faastermetrics as fm
from faastermetrics.helper import group_by
from faastermetrics.logentry import ColdstartLog
from faastermetrics.calls import create_requestgroups

sns.set_style("whitegrid")


def timedelta_to_ms(timedelta):
    return timedelta.total_seconds() * 1000


def plot_kdeplot(data, ylabel, title, log_scaling=True):
    """
    Args:
        data: Dict of list of numbers.
    """

    fig, ax = plt.subplots(1, 1, figsize=(8, 6), dpi=300)
    plt.xticks(rotation=90)
    d = []
    for k, v in data.items():
        for i in v:
            d.append((k, i))
    df = pd.DataFrame(d, columns=["name", "inv"])

    for func_name in data.keys():
        if func_name in ["email", "payment"]:
            continue
        # Subset to the airline
        subset = df[df['name'] == func_name]

        # Draw the density plot
        plt.hist(subset['inv'], label=func_name, ax=ax)

    ax.set_ylabel(ylabel)
    ax.set_title(title)

    if log_scaling:
        ax.set_yscale("log")

    fig.tight_layout()

    return fig


def plot_function_coldstarts(data, plot_dir):
    cld_groups = [d for d in data if isinstance(d, ColdstartLog)]
    fn_calls = group_by(cld_groups, lambda c: c.function)

    function_coldstarts = {}
    for function, calls in fn_calls.items():
        function_coldstarts[function] = [
            c.timestamp for c in calls if c.timestamp is not None
        ]

    fig = plot_kdeplot(
        function_coldstarts,
        ylabel="Coldstart Count",
        title="KDE of function coldstarts",
        log_scaling=True
    )

    plot_path = str(plot_dir / "function_coldstarts.png")
    print(f"Plotting to {plot_path}")
    fig.savefig(plot_path)


def plot_function_execution_time(data, plot_dir):
    cgroups = create_requestgroups(data)
    fn_calls = group_by(cgroups, lambda c: c.function)
    function_durations = {}
    for function, calls in fn_calls.items():
        if function not in ("artillery", None):
            function_durations[function] = [
                c.start_time for c in calls if c.start_time is not None
            ]

    fig = plot_kdeplot(
        function_durations,
        ylabel="Invocation Count",
        title="KDE of function invocations",
        log_scaling=True
    )

    plot_path = str(plot_dir / "function_invocations.png")
    print(f"Plotting to {plot_path}")
    fig.savefig(plot_path)


def main(input_data: pathlib.Path, plot_dir: pathlib.Path):
    plot_dir = plot_dir / input_data.stem
    plot_dir.mkdir(exist_ok=True, parents=True)

    data = fm.load_logs(input_data)

    plot_function_coldstarts(data, plot_dir)
    # plot_function_execution_time(data, plot_dir)


if __name__ == "__main__":
    argmagic(main, positional=("input_data", "plot_dir"))
