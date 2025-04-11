from typing import Any

from dbt_common.events.base_types import BaseEvent
from dbt_common.events.functions import fire_event
from dbt_common.events.types import RecordReplayIssue

from dbt.adapters.contracts.connection import Connection
from dbt.adapters.record.cursor.cursor import RecordReplayCursor


class RecordReplayHandle:
    """A proxy object used for record/replay modes. What adapters call a
    'handle' is typically a native database connection, but should not be
    confused with the Connection protocol, which is a dbt-adapters concept.

    Currently, the only function of the handle proxy is to provide a record/replay
    aware cursor object when cursor() is called."""

    def __init__(self, native_handle: Any, connection: Connection) -> None:
        self.native_handle = native_handle
        self.connection = connection

    def cursor(self) -> Any:
        # The native handle could be None if we are in replay mode, because no
        # actual database access should be performed in that mode.
        cursor = None if self.native_handle is None else self.native_handle.cursor()
        return RecordReplayCursor(cursor, self.connection)

    def commit(self):
        self.native_handle.commit()

    def rollback(self):
        self.native_handle.rollback()

    def close(self):
        self.native_handle.close()

    def get_backend_pid(self):
        return self.native_handle.get_backend_pid()

    @property
    def closed(self):
        return self.native_handle.closed

    def _fire_event(self, evt: BaseEvent) -> None:
        """Wraps fire_event for easier test mocking."""
        fire_event(evt)

    def __getattr__(self, name: str) -> Any:
        self._fire_event(
            RecordReplayIssue(
                msg=f"Unexpected attribute '{name}' accessed on {self.__class__.__name__}"
            )
        )
        return getattr(self.native_handle, name)
