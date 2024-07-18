from typing import Any, Optional

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
        self.native_cursor.execute(operation, parameters)

    @record_function(CursorFetchOneRecord, method=True, id_field_name="connection_name")
    def fetchone(self) -> Any:
        return self.native_cursor.fetchone()

    @record_function(CursorFetchManyRecord, method=True, id_field_name="connection_name")
    def fetchmany(self, size: int) -> Any:
        return self.native_cursor.fetchmany(size)

    @record_function(CursorFetchAllRecord, method=True, id_field_name="connection_name")
    def fetchall(self) -> Any:
        return self.native_cursor.fetchall()

    @property
    def connection_name(self) -> Optional[str]:
        return self.connection.name

    @property
    @record_function(CursorGetRowCountRecord, method=True, id_field_name="connection_name")
    def rowcount(self) -> int:
        return self.native_cursor.rowcount

    @property
    @record_function(CursorGetDescriptionRecord, method=True, id_field_name="connection_name")
    def description(self) -> str:
        return self.native_cursor.description
