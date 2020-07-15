"""
Performance logging utility functions.
"""
from typing import List
import datetime
from dataclasses import dataclass, field

from .logentry import LogEntry, RequestLog, PerfLog, UNDEFINED_XPAIR, MARK_END, MARK_START, ArtilleryLog
from . import helper as cg
from .helper import group_by_function, group_by, uniq_by


@dataclass
class Call:
    id: tuple
    function: str
    duration: datetime.timedelta
    entries: List[LogEntry]
    calls: List["Call"] = field(default_factory=lambda: list())

    @property
    def start_time(self):
        if len(self.entries) == 0:
            return None
        return sorted(self.entries, key=lambda e: e.timestamp)[0]

    @property
    def end_time(self):
        if len(self.entries) == 0:
            return None
        return sorted(self.entries, key=lambda e: e.timestamp)[-1]

    @property
    def log_duration(self):
        if self.end_time is None or self.start_time is None:
            return None
        return self.end_time - self.start_time

    def __repr__(self):
        fmt_id = "-".join(map(str, self.id))
        call_str = f"{self.function}:{fmt_id}:({len(self.calls)} subcalls)"
        all_calls = [call_str]
        for subcall in self.calls:
            all_calls.append("  " + repr(subcall))
        return "\n".join(all_calls)


def artillery_to_call(entries: List[LogEntry]) -> Call:
    id, = uniq_by(entries, lambda e: e.id)
    called_id, = uniq_by(entries, lambda e: e.called_id)
    start = cg.get_one(entries, lambda e: e.event["type"] == "before")
    end = cg.get_one(entries, lambda e: e.event["type"] == "after")
    duration = end.timestamp - start.timestamp
    function, = uniq_by(entries, lambda e: e.url)

    calls = [
        Call(id=called_id, function=function, entries=[], duration=duration)
    ]
    return Call(
        id=id,
        function="artillery",
        duration=duration,
        calls=calls,
        entries=entries,
    )


def single_request_to_call(entries: List[LogEntry], id=None, function=None) -> Call:
    if id is None:
        id, = uniq_by(entries, lambda e: e.id)
    if function is None:
        function, = uniq_by(entries, lambda e: e.fn["name"])

    measure = cg.get_one(entries, lambda e: e.perf["entryType"] == "measure")
    duration = datetime.timedelta(milliseconds=measure.perf["duration"])

    return Call(
        id=id,
        function=function,
        duration=duration,
        entries=entries,
    )


def get_rpc_out_function(entries):
    fun, = uniq_by(entries, lambda e: e.perf_type_data.split(":")[0])
    return fun


def get_rpc_out_id(entries):
    id, = uniq_by(entries, lambda e: tuple(e.perf_type_data.split(":")[1].split("-")))
    return id


def request_to_call(entries: List[LogEntry]) -> Call:
    perf_entries = [e for e in entries if isinstance(e, PerfLog)]
    incoming_entries = [e for e in perf_entries if PerfLog.is_incoming_entry(e)]
    outgoing_entries = [e for e in perf_entries if PerfLog.is_outgoing_entry(e)]
    call = single_request_to_call(incoming_entries)
    call.calls = [
        single_request_to_call(e, get_rpc_out_id(e), get_rpc_out_function(e))
        for e in group_by(outgoing_entries, lambda e: e.perf_type_data).values()
    ]
    request_entries = [e for e in entries if isinstance(e, RequestLog)]
    if len(request_entries) > 1:
        raise ValueError(f"Too many request entries in single group: {request_entries}")
    call.entries += request_entries
    return call


def misc_to_call(entries: List[LogEntry]) -> Call:
    return Call(
        id=(None, UNDEFINED_XPAIR),
        function=None,
        duration=None,
        entries=entries,
    )


def id_groups_to_call(entry_id, entries: List[LogEntry]) -> Call:
    if entry_id[0] is None:
        return misc_to_call(entries)

    if all(isinstance(e, ArtilleryLog) for e in entries):
        return artillery_to_call(entries)

    return request_to_call(entries)


def url_to_function_name(name):
    # AWS URL is DOMAIN/dev/PATH
    if "/dev/" in name:
        parts = name.split("/dev/", 1)[1].split("/")
    else:
        parts = name.split("/", 1)[1:]

    parts = [p for p in parts if p]

    new_name = "/".join(parts[:2])

    return new_name


def normalize_call_names(calls):
    id_names = group_by(
        [c for c in calls] + [s for c in calls for s in c.calls],
        lambda c: c.id
    )

    # built id name translation mapping
    id_translated = {}
    for key, id_calls in id_names.items():
        names = {c.function for c in id_calls}
        if len(names) == 1:
            name, = names
            if "http" in name:
                name = url_to_function_name(name)
            id_translated[key] = name
        elif len(names) > 1:
            name, = [url_to_function_name(n) for n in names if "http" in n]
            id_translated[key] = name

    # rename calls
    for call in calls:
        call.function = id_translated[call.id]
        for subcall in call.calls:
            subcall.function = id_translated[subcall.id]

    return calls


def create_requestgroups(data: List[LogEntry]) -> List[Call]:
    """Create a list of logs based on request behavior."""
    context_id_groups = group_by(data, lambda e: e.id)
    calls = [
        id_groups_to_call(id, entries)
        for id, entries in context_id_groups.items()
    ]

    # remove calls without a context ID, these are most probably platform
    # messages
    len_all_calls = len(calls)
    calls = [c for c in calls if c.id[0] is not None]
    print(f"Keep with context ids only: {len(calls)}/{len_all_calls}")

    # normalize artillery call urls
    calls = normalize_call_names(calls)
    return calls
