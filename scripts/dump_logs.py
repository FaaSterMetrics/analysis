#!/usr/bin/env python3
import json
import pathlib
import datetime
from collections import Counter

from argmagic import argmagic

import faastermetrics as fm


def parse_timewindow(timewindow: str) -> datetime.timedelta:
    tdelta_args = {}
    if timewindow.endswith("s"):
        tdelta_args["seconds"] = int(timewindow[:-1])
    elif timewindow.endswith("m"):
        tdelta_args["minutes"] = int(timewindow[:-1])
    elif timewindow.endswith("h"):
        tdelta_args["hours"] = int(timewindow[:-1])
    elif timewindow.endswith("d"):
        tdelta_args["days"] = int(timewindow[:-1])
    else:
        raise ValueError(f"Time window should be number with d/h/m/s suffix (eg 30h) but got: {timewindow}")
    return datetime.timedelta(**tdelta_args)


def dump_logs(
        logdir: pathlib.Path,
        outdir: pathlib.Path,
        version: str = None,
        timewindow: str = None,
):
    """Output logs to the given destination directory.

    Args:
        logdir: Directory containing raw log entries.
        outdir: Destination for outputting collected log json.
        version: Version requirement for inclusion.
        timewindow: Only include events inside a timewindow up to latest.
    """
    log_entries = fm.parse_logdir(logdir)
    print(f"Loading {len(log_entries)} entries from {logdir}")

    if version is not None:
        print(f"Filtering: version={version}")
        num_before = len(log_entries)
        log_entries = list(filter(lambda e: e.data["version"] == version, log_entries))
        num_after = len(log_entries)
        print(f"  Kept {num_after}/{num_before} ({num_before - num_after} removed)")

    if timewindow is not None:
        end_time = max(*[e.timestamp for e in log_entries])
        timedelta = parse_timewindow(timewindow)
        start_time = end_time - timedelta
        print(f"Filtering timewindow of {timewindow}: {start_time} {end_time}")
        num_before = len(log_entries)
        log_entries = [e for e in log_entries if e.timestamp >= start_time]
        num_after = len(log_entries)
        print(f"  Kept {num_after}/{num_before} ({num_before - num_after} removed)")

    deploy_path = logdir / "deployment_id.txt"
    if deploy_path.exists():
        with open(deploy_path) as dfile:
            deploy_id = dfile.read().strip()
        print(f"Filtering on deploy ID: {deploy_id}")
        num_before = len(log_entries)
        log_entries = [e for e in log_entries if e.data.get("deploymentId", "") == deploy_id]
        num_after = len(log_entries)
        print(f"  Kept {num_after}/{num_before} ({num_before - num_after} removed)")

    if outdir.is_dir():
        outdir = outdir / f"{logdir.name}.json"

    print(f"Dumping {len(log_entries)} entries to {outdir}")
    with open(outdir, "w") as jsfile:
        json.dump(log_entries, jsfile)


if __name__ == "__main__":
    argmagic(dump_logs, positional=("logdir", "outdir"))
