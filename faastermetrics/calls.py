"""
Performance logging utility functions.
"""
from typing import List
import datetime
from dataclasses import dataclass, field

from .logentry import LogEntry, RequestLog, PerfLog, UNDEFINED_XPAIR, MARK_END, MARK_START
from . import helper as cg
from .helper import group_by_function, group_by, uniq_by


@dataclass
class Call:
    id: tuple
    function: str
    start_time: datetime.datetime
    end_time: datetime.datetime
    entries: List[LogEntry]
    calls: List["Call"] = field(default_factory=lambda: list())

    @property
    def duration(self):
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
    start = cg.get_one(entries, lambda e: e.event["type"] == "before")
    end = cg.get_one(entries, lambda e: e.event["type"] == "after")
    return Call(
        id=id,
        function="artillery",
        start_time=start.timestamp,
        end_time=end.timestamp,
        entries=entries,
    )


def single_request_to_call(entries: List[LogEntry]) -> Call:
    id, = uniq_by(entries, lambda e: e.id)
    function, = uniq_by(entries, lambda e: e.fn["name"])

    # set routed path if CRUD call
    if all(PerfLog.is_routed_entry(e) for e in entries):
        subname = entries[0].perf_type_data.split(":")[0]
        if subname != "/":
            function += subname

    start = cg.get_one(entries, lambda e: e.perf_type[0] == MARK_START)
    end = cg.get_one(entries, lambda e: e.perf_type[0] == MARK_END)
    return Call(
        id=id,
        function=function,
        start_time=start.timestamp,
        end_time=end.timestamp,
        entries=entries,
    )


def request_to_call(entries: List[LogEntry]) -> Call:
    perf_entries = [e for e in entries if isinstance(e, PerfLog)]
    incoming_entries = [e for e in perf_entries if PerfLog.is_incoming_entry(e)]
    outgoing_entries = [e for e in perf_entries if PerfLog.is_outgoing_entry(e)]
    call = single_request_to_call(incoming_entries)
    call.calls = [
        single_request_to_call(e)
        for e in group_by(outgoing_entries, lambda e: e.perf_type_data).values()
    ]
    return call


def misc_to_call(entries: List[LogEntry]) -> Call:
    return Call(
        id=(None, UNDEFINED_XPAIR),
        function=None,
        start_time=None,
        end_time=None,
        entries=entries,
    )


def id_groups_to_call(entries: List[LogEntry]) -> Call:
    (context_id, _), = uniq_by(entries, lambda e: e.id)
    if context_id is None:
        return misc_to_call(entries)

    function, = uniq_by(entries, lambda e: e.fn["name"])
    if function == "artillery":
        return artillery_to_call(entries)

    if any(isinstance(e, RequestLog) for e in entries):
        return request_to_call(entries)


def create_requestgroups(data: List[LogEntry]) -> List[Call]:
    """Create a list of logs based on request behavior."""
    context_id_groups = group_by(data, lambda e: (e.id, e.function))
    return [id_groups_to_call(entries) for _, entries in context_id_groups.items()]
