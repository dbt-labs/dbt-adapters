from typing import Any

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
