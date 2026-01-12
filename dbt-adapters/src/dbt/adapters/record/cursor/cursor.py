from typing import Any, Optional

from dbt_common.events.base_types import BaseEvent
from dbt_common.events.functions import fire_event
from dbt_common.events.types import RecordReplayIssue
from dbt_common.record import record_function

from dbt.adapters.contracts.connection import Connection
from dbt.adapters.record.cursor.description import CursorGetDescriptionRecord
from dbt.adapters.record.cursor.execute import CursorExecuteRecord
from dbt.adapters.record.cursor.fetchone import CursorFetchOneRecord
from dbt.adapters.record.cursor.fetchmany import CursorFetchManyRecord
from dbt.adapters.record.cursor.fetchall import CursorFetchAllRecord
from dbt.adapters.record.cursor.rowcount import CursorGetRowCountRecord


class RecordReplayCursor:
    """A proxy object used to wrap native database cursors under record/replay
    modes. In record mode, this proxy notes the parameters and return values
    of the methods and properties it implements, which closely match the Python
    DB API 2.0 cursor methods used by many dbt adapters to interact with the
    database or DWH. In replay mode, it mocks out those calls using previously
    recorded calls, so that no interaction with a database actually occurs."""

    def __init__(self, native_cursor: Any, connection: Connection) -> None:
        self.native_cursor = native_cursor
        self.connection = connection

    @record_function(CursorExecuteRecord, method=True, id_field_name="connection_name")
    def execute(self, operation, parameters=None) -> None:
        # In replay mode, native_cursor may be None. This code path is only reached
        # if the record was found and returned by the decorator, or if we're recording.
        # If native_cursor is None here, it means we're in an unexpected state.
        if self.native_cursor is None:
            self._fire_event(
                RecordReplayIssue(
                    msg=f"execute() called with None cursor. Operation: {operation[:100]}..."
                )
            )
            return
        self.native_cursor.execute(operation, parameters)

    @record_function(CursorFetchOneRecord, method=True, id_field_name="connection_name")
    def fetchone(self) -> Any:
        if self.native_cursor is None:
            self._fire_event(
                RecordReplayIssue(msg="fetchone() called with None cursor")
            )
            return None
        return self.native_cursor.fetchone()

    @record_function(CursorFetchManyRecord, method=True, id_field_name="connection_name")
    def fetchmany(self, size: int) -> Any:
        if self.native_cursor is None:
            self._fire_event(
                RecordReplayIssue(msg=f"fetchmany({size}) called with None cursor")
            )
            return []
        return self.native_cursor.fetchmany(size)

    @record_function(CursorFetchAllRecord, method=True, id_field_name="connection_name")
    def fetchall(self) -> Any:
        if self.native_cursor is None:
            self._fire_event(
                RecordReplayIssue(msg="fetchall() called with None cursor")
            )
            return []
        return self.native_cursor.fetchall()

    @property
    def connection_name(self) -> Optional[str]:
        return self.connection.name

    @property
    @record_function(CursorGetRowCountRecord, method=True, id_field_name="connection_name")
    def rowcount(self) -> int:
        if self.native_cursor is None:
            return 0
        return self.native_cursor.rowcount

    @property
    @record_function(CursorGetDescriptionRecord, method=True, id_field_name="connection_name")
    def description(self) -> str:
        if self.native_cursor is None:
            return None
        return self.native_cursor.description

    def _fire_event(self, evt: BaseEvent) -> None:
        """Wraps fire_event for easier test mocking."""
        fire_event(evt)

    def __getattr__(self, name: str) -> Any:
        self._fire_event(
            RecordReplayIssue(
                msg=f"Unexpected attribute '{name}' accessed on {self.__class__.__name__}"
            )
        )
        # In replay mode, native_cursor may be None
        if self.native_cursor is None:
            return None
        return getattr(self.native_cursor, name)
