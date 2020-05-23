import json
import pathlib
import datetime
from dataclasses import dataclass

from typing import List


from argmagic import argmagic


@dataclass
class LogEntry:
    timestamp: datetime.datetime
    message: str


def parse_aws_entry(raw_entry: dict) -> LogEntry:
    """Parse an AWS CloudWatch formatted JSON message."""
    timestamp = datetime.datetime.fromtimestamp(int(raw_entry["timestamp"] / 1000))
    try:
        message = json.loads(raw_entry["message"])
    except ValueError:
        message = None
    return LogEntry(timestamp=timestamp, message=message)


def parse_google_entry(raw_entry: dict) -> LogEntry:
    """Parse an Google Cloud formatted JSON message."""
    timestamp = datetime.datetime.fromisoformat(raw_entry["timestamp"][:-1])
    message = raw_entry.get("jsonPayload", None) or raw_entry.get("protoPayload", None)
    return LogEntry(timestamp=timestamp, message=message)



def parse_entry(raw_entry: dict) -> LogEntry:
    if "ingestionTime" in raw_entry:
        return parse_aws_entry(raw_entry)
    elif "receiveTimestamp" in raw_entry:
        return parse_google_entry(raw_entry)
    else:
        raise ValueError(f"Unknown entry format for {raw_entry}")


def is_valid(entry: LogEntry) -> bool:
    return isinstance(entry.message, dict)


def read_json_log(path: pathlib.Path) -> List[dict]:
    """Read json logs at the given path."""
    with open(path) as f:
        data = [json.loads(line) for line in f]

    parsed_entries = [parse_entry(d) for d in data]

    valid_entries = [e for e in parsed_entries if is_valid(e)]
    return valid_entries


def main(logdir: pathlib.Path):
    """
    Simple script for some initial testing on logging data.

    Args:
        logdir: Directory containing logging data.
    """

    files = {p.stem: read_json_log(p) for p in logdir.iterdir()}

    print("= Available logs =")
    for name, lines in files.items():
        print(f"{name}: {len(lines)} entries")
        for line in lines:
            print(line.timestamp)
            print(line.message)


if __name__ == "__main__":
    argmagic(main, positional=("logdir",))
