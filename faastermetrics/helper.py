from typing import List, Callable, Dict
from collections import defaultdict, Counter

from .logentry import LogEntry


def repr_type_count(data: list) -> List[str]:
    return [
        f"{k}({v})" for k, v in Counter(
            [e.__class__.__name__ for e in data]
        ).items()
    ]


def print_group(grouped_data: dict):
    """Print the group. Mainly for debugging purposes."""
    lines = [
        "### Grouped data overview",
    ]

    for name, entries in grouped_data.items():
        title = "# {}: {} entries".format(name, len(entries))
        types = "#   " + ", ".join(
            f"{k}({v})" for k, v in Counter([e.__class__.__name__ for e in entries]).items()
        )
        lines += (title, types)
    lines.append("###")

    text = "\n".join(lines)

    print(text)


def group_by(data: List[LogEntry], key: Callable) -> Dict[str, List[LogEntry]]:
    """Group by the given key or callable."""
    grouped = defaultdict(list)
    for entry in data:
        grouped[key(entry)].append(entry)
    return grouped


def uniq_by(data: List[LogEntry], key: Callable) -> list:
    """Get all unique values for the given accessed value."""
    return list({key(d) for d in data})


def group_by_context(entries):
    return group_by(entries, lambda e: e.context_id)


def group_by_function(entries):
    return group_by(entries, lambda e: e.fn["name"])
