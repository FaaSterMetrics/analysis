import json
import pathlib
import datetime
from typing import List

from dataclasses import dataclass


MESSAGE_TAG = "FAASTERMETRICS"


@dataclass
class LogEntry:
    timestamp: datetime.datetime
    data: dict


def parse_logfile(path: pathlib.Path) -> List[LogEntry]:
    """Read json logs at the given path."""
    with open(path) as f:
        parsed_entries = [_parse_entry(line) for line in f]

    valid_entries = [e for e in parsed_entries if _is_valid(e)]
    return valid_entries


def _parse_entry(raw_entry: str) -> LogEntry:
    start_pos = raw_entry.find(MESSAGE_TAG)
    decoder = json.JSONDecoder()
    if start_pos == -1:
        return None
    json_start = start_pos + len(MESSAGE_TAG)

    raw_entry = raw_entry.encode("utf-8").decode("unicode_escape")
    obj, _ = decoder.raw_decode(raw_entry[json_start:])

    timestamp = datetime.datetime.fromtimestamp(obj["timestamp"] / 1000)

    entry = LogEntry(timestamp, obj)
    return entry


def _is_valid(entry: LogEntry) -> bool:
    return entry is not None
