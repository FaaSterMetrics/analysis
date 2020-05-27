import json
import pathlib
import datetime
from typing import List

from dataclasses import dataclass

from json_coder import jsonify, register


__version__ = "0.5.0"

register("datetime", datetime.datetime, datetime.datetime.fromisoformat, datetime.datetime.isoformat)


MESSAGE_TAG = "FAASTERMETRICS"


@jsonify("logentry")
@dataclass
class LogEntry:
    timestamp: datetime.datetime
    data: dict
    platform: str

    @property
    def event(self):
        return self.data["event"]


def load_logs(logdump: pathlib.Path) -> List[LogEntry]:
    """Load dumped logs in json format.

    This is an alternative to just directly using json.load on a opened file.
    """
    with open(logdump, "r") as logfile:
        entries = json.load(logdump)
    return entries

def is_log_folder(logdir: pathlib.Path) -> bool:
    """Check whether the given folder is a valid log directory, eg whether aws,
    gcp logs etc are contained."""
    num_logs = sum(1 for p in logdir.iterdir() if p.suffix == ".log")
    return num_logs > 0


def parse_logdir(path: pathlib.Path) -> List[LogEntry]:
    if not is_log_folder(path):
        raise ValueError(f"{path} is not a valid log directory.")
    entries = [
        entry
        for filepath in path.iterdir() for entry in parse_logfile(filepath, platform=filepath.stem)
    ]
    return entries


def parse_logfile(path: pathlib.Path, platform: str = None) -> List[LogEntry]:
    """Read json logs at the given path."""
    if platform is None:
        # Parse platform from name of logfile
        platform = path.stem

    with open(path) as f:
        parsed_entries = [_parse_entry(line, platform) for line in f]

    valid_entries = [e for e in parsed_entries if _is_valid(e)]
    return valid_entries


def _parse_entry(raw_entry: str, platform: str) -> LogEntry:
    start_pos = raw_entry.find(MESSAGE_TAG)
    decoder = json.JSONDecoder()
    if start_pos == -1:
        return None
    json_start = start_pos + len(MESSAGE_TAG)

    raw_entry = raw_entry.encode("utf-8").decode("unicode_escape")
    obj, _ = decoder.raw_decode(raw_entry[json_start:])

    timestamp = datetime.datetime.fromtimestamp(obj["timestamp"] / 1000)

    entry = LogEntry(timestamp, obj, platform)
    return entry


def _is_valid(entry: LogEntry) -> bool:
    return (entry is not None) and ("event" in entry.data) and ("fn" in entry.data["event"])
