"""
Performance logging utility functions.
"""
from typing import List
from dataclasses import dataclass, field

from .logentry import LogEntry, RequestLog, PerfLog
from . import contextgroup as cg
from .contextgroup import group_by_function, group_by_context, uniq_by


@dataclass
class RequestGroup:
    entries: List[LogEntry]
    function: str = field(init=False)
    context_id: str = field(init=False)
    request: RequestLog = field(init=False)
    perf: List[PerfLog] = field(init=False)

    def __post_init__(self):
        self.function, = uniq_by(self.entries, lambda e: e.fn["name"])
        self.context_id, = uniq_by(self.entries, lambda e: e.context_id)

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


def create_requestgroups(data: List[LogEntry]) -> List[RequestGroup]:
    """Create a list of logs based on request behavior."""
    context_id_groups = group_by_context(data)

    request_groups = []
    for context_id, entries in context_id_groups.items():
        function_entries = group_by_function(entries)
        for name, fentries in function_entries.items():
            req_group = RequestGroup(fentries)
            request_groups.append(req_group)

    return request_groups
