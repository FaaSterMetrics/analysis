#!/usr/bin/env python3
import pathlib
from dataclasses import asdict

import pandas as pd
from argmagic import argmagic

import faastermetrics as fm

EXPORTERS = {
    ".csv": lambda frame, target: frame.to_csv(target),
    ".xlsx": lambda frame, target: frame.to_xlsx(target)
}


def main(input_data: pathlib.Path, out_name: pathlib.Path):
    """Exports a log dump to a given format.

    Args:
        input_data: File containing raw log entries.
        out_name: Destination path for the export, currently supported extensions are [.cvs, .xlsx]
    """
    if out_name.suffix not in EXPORTERS:
        print(f"Unknown extension {out_name.suffix}")
        return
    data = fm.load_logs(input_data)
    df = pd.json_normalize(map(asdict, data))
    EXPORTERS[out_name.suffix](df, out_name)


if __name__ == "__main__":
    argmagic(main, positional=("input_data", "out_name"))
