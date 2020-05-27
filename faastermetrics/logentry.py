import datetime
from typing import Union
from dataclasses import dataclass, asdict

from json_coder import jsonify


@jsonify("logentry")
@dataclass
class LogEntry:
    timestamp: datetime.datetime
    data: dict
    platform: str

    @property
    def event(self):
        return self.data["event"]


class RequestLog(LogEntry):
    @property
    def request(self):
        return self.event["request"]

    @property
    def context_id(self):
        return self.request["headers"].get("x-context", None)


class PerfLog(LogEntry):
    @property
    def perf(self):
        return self.event["perf"]

    @property
    def context_id(self):
        mark = self.perf["mark"]
        return mark.split(":")[0]


def cast_log_type(entry: LogEntry) -> Union[RequestLog, PerfLog]:
    if "perf" in entry.event:
        return PerfLog(**asdict(entry))
    if "request" in entry.event:
        return RequestLog(**asdict(entry))
    raise TypeError("Unknown log type.")
