"""
Performance logging utility functions.
"""
from typing import List
from dataclasses import dataclass, field

from .logentry import LogEntry, RequestLog, PerfLog
from . import helper as cg
from .helper import group_by_function, group_by, uniq_by


@dataclass
class RequestGroup:
    entries: List[LogEntry]
    function: str = field(init=False)
    context_id: str = field(init=False)
    x_pair: str = field(init=False)
    request: RequestLog = field(init=False)
    perf: List[PerfLog] = field(init=False)

    def __post_init__(self):
        self.function, = uniq_by(self.entries, lambda e: e.fn["name"])
        self.context_id, = uniq_by(self.entries, lambda e: e.context_id)
        # self.x_pair, = uniq_by(self.entries, lambda e: e.x_pair)

        if self.context_id is not None:
            self.request, = filter(lambda e: isinstance(e, RequestLog), self.entries)
        else:
            self.request = None

        self.perf = list(filter(lambda e: isinstance(e, PerfLog), self.entries))

    def get_rpc_out(self):
        """Get outgoing RPC function calls."""
        return list(filter(lambda e: "rpcOut" in e.perf_name, self.perf))

    def get_rpc_in(self):
        """Get outgoing RPC function calls."""
        return list(filter(lambda e: "rpcIn" in e.perf_name, self.perf))

    def __repr__(self):
        req_types = ", ".join(cg.repr_type_count(self.entries))
        return f"{self.function}: {self.context_id}: {req_types}"


def split_request_sequential(entries):
    entries = sorted(entries, key=lambda e: e.timestamp)
    collected = []
    request_seen = False
    for entry in entries:
        if isinstance(entry, RequestLog):
            if request_seen:
                yield collected.copy()
                collected = [entry]
            else:
                request_seen = True
                collected.append(entry)
        else:
            collected.append(entry)

    yield collected


def create_requestgroups(data: List[LogEntry]) -> List[RequestGroup]:
    """Create a list of logs based on request behavior."""
    # context_id_groups = group_by(data, lambda e: e.id)
    context_id_groups = group_by(data, lambda e: e.context_id)

    request_groups = []
    for key, entries in context_id_groups.items():
        function_entries = group_by_function(entries)
        for name, fentries in function_entries.items():
            try:
                req_group = RequestGroup(fentries)
                request_groups.append(req_group)
            except ValueError as e:
                print(f"Error generating group for {key}/{name}: {e}")
                for segment in split_request_sequential(fentries):
                    req_group = RequestGroup(segment)
                    request_groups.append(req_group)

    return request_groups
