from typing import Union

from .logentry import RequestLog, PerfLog


class ContextGroup:
    """A group of log entries that form a single function call."""
    def __init__(self):
        self._entries = []
        self._context_id = None

    @property
    def entries(self):
        return self._entries.copy()

    @property
    def context_id(self):
        return self._context_id

    def add(self, entry: Union[RequestLog, PerfLog]) -> bool:
        """Add entries to the context group, return True/False based on whether
        the added entry was valid or not."""
        if self.context_id is not None and entry.context_id is None:
            return False

        new_entries = self._entries + [entry]
        try:
            merged_id = self._merge_context_ids(new_entries)
        except ValueError:
            return False

        if (self._context_id is not None) <= (self._context_id == merged_id):
            self._context_id = merged_id
            self._entries = new_entries
        else:
            return False

        return True

    @staticmethod
    def _merge_context_ids(entries):
        """Attempt to merge given logs into a single call ID."""
        context_ids = {e.context_id for e in entries if e.context_id is not None}
        if len(context_ids) == 1:
            return next(iter(context_ids))
        elif len(context_ids) == 0:
            return None
        else:
            raise ValueError("Ambiguous context ID")

    def __repr__(self):
        return f"Context({self.context_id}):{len(self._entries)}"


def create_context_groups(entries):
    context_groups = []
    context_group = ContextGroup()
    for entry in sorted(entries, key=lambda e: e.timestamp):
        if not context_group.add(entry):
            context_groups.append(context_group)
            context_group = ContextGroup()
            context_group.add(entry)
    return context_groups
