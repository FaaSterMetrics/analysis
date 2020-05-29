import datetime
from typing import Union, ClassVar
from dataclasses import dataclass, asdict

from json_coder import jsonify


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
    def event(self):
        return self.data["event"]

    @property
    def fn(self):
        return self.data["fn"]

    @classmethod
    def match(cls, event):
        if not cls._special_keys:
            raise NotImplementedError("Cannot match log data without defined identifying keys.")
        return all(key in event.event for key in cls._special_keys)


class RequestLog(LogEntry):
    _special_keys: ClassVar = ("request",)

    @property
    def request(self):
        return self.event["request"]

    @property
    def context_id(self):
        return self.request["headers"].get("x-context", None)


class PerfLog(LogEntry):
    _special_keys: ClassVar = ("perf",)

    @property
    def perf(self):
        return self.event["perf"]

    @property
    def context_id(self):
        mark = self.perf["mark"]
        return mark.split(":")[0]


class ColdstartLog(LogEntry):
    _special_keys: ClassVar = ("coldstart",)

    @property
    def coldstart(self):
        return self.event["coldstart"]


def cast_log_type(entry: LogEntry) -> Union[RequestLog, PerfLog]:
    for subtype in LOG_SUBTYPES:
        if subtype.match(entry):
            return subtype(**asdict(entry))
    print(f"Unknown log type: {entry}")
    return entry
