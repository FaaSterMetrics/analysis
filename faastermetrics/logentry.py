import datetime
from typing import Union, ClassVar
from dataclasses import dataclass, asdict

from json_coder import jsonify


UNDEFINED_XPAIR = "undefined-x-pair"
ROUTED_TYPES = ("get", "post", "put", "patch", "del", "all")
INCOMING_REQ_TYPES = ("rpcIn", *ROUTED_TYPES)
OUTGOING_REQ_TYPES = ("rpcOut",)
MARK_START = "start"
MARK_END = "end"


LOG_SUBTYPES = []


class LogMeta(type):
    def __new__(cls, name, bases, attrs):
        new_cls = type.__new__(cls, name, bases, attrs)
        if attrs["_special_keys"]:
            LOG_SUBTYPES.append(new_cls)
        return new_cls


@jsonify("logentry")
@dataclass
class LogEntry(metaclass=LogMeta):
    _special_keys: ClassVar = tuple()  # ignore field for dataclasses
    timestamp: datetime.datetime
    data: dict
    platform: str

    @property
    def id(self):
        return (self.context_id, self.x_pair)

    @property
    def event(self):
        return self.data["event"]

    @property
    def fn(self):
        return self.data["fn"]

    @property
    def function(self):
        return self.fn["name"]

    @classmethod
    def match(cls, event):
        if not cls._special_keys:
            raise NotImplementedError("Cannot match log data without defined identifying keys.")
        return all(key in event.event for key in cls._special_keys)

    @property
    def x_pair(self):
        label_xpair = self.event.get("xPair", UNDEFINED_XPAIR)
        if label_xpair == UNDEFINED_XPAIR:
            return UNDEFINED_XPAIR
        _, xpair = label_xpair.split("-")
        return xpair

    @property
    def context_id(self):
        return self.event.get("contextId", None)


class RequestLog(LogEntry):
    _special_keys: ClassVar = ("request",)

    @property
    def request(self):
        return self.event["request"]


class PerfLog(LogEntry):
    _special_keys: ClassVar = ("perf",)

    @property
    def perf(self):
        return self.event["perf"]

    @property
    def type(self):
        return self.perf["entryType"]

    @property
    def perf_type(self):
        mark_type, perf_type, *_ = self.perf["mark"].split(":")
        return (mark_type, perf_type)

    @property
    def perf_type_data(self):
        perfs = self.perf["mark"].split(":")
        if len(perfs) < 3:
            return ""
        return ":".join(perfs[2:])

    @property
    def perf_name(self):
        return self.perf["name"]

    def _get_perf_name(self):
        splitted = self.perf_name.split(":")
        fname, context_id, xpair, *perftype = splitted
        perftype = ":".join(perftype)
        return fname, context_id, xpair, perftype

    @staticmethod
    def is_incoming_entry(entry):
        return any(t == entry.perf_type[1] for t in INCOMING_REQ_TYPES)

    @staticmethod
    def is_routed_entry(entry):
        return any(t == entry.perf_type[1] for t in ROUTED_TYPES)

    @staticmethod
    def is_outgoing_entry(entry):
        return any(t == entry.perf_type[1] for t in OUTGOING_REQ_TYPES)


class ColdstartLog(LogEntry):
    _special_keys: ClassVar = ("coldstart",)

    @property
    def coldstart(self):
        return self.event["coldstart"]


class ArtilleryLog(LogEntry):
    _special_keys: ClassVar = ("url", "type")

    @property
    def called_id(self):
        return super().id

    @property
    def id(self):
        return (self.context_id, UNDEFINED_XPAIR)

    @property
    def type(self):
        return self.event["type"]

    @property
    def url(self):
        return self.event["url"]


def cast_log_type(entry: LogEntry) -> Union[RequestLog, PerfLog]:
    for subtype in LOG_SUBTYPES:
        if subtype.match(entry):
            return subtype(**asdict(entry))
    print(f"Unknown log type: {entry}")
    return entry
