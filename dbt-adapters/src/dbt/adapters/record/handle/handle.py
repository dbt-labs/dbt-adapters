from typing import Any, Optional

from dbt_common.record import record_function

from dbt.adapters.contracts.connection import Connection

from dbt.adapters.record.cursor.cursor import RecordReplayCursor
from dbt.adapters.record.handle.close import HandleCloseRecord
from dbt.adapters.record.handle.closed import HandleGetClosedRecord
from dbt.adapters.record.handle.commit import HandleCommitRecord
from dbt.adapters.record.handle.get_backend_pid import HandleGetBackendPidRecord
from dbt.adapters.record.handle.rollback import HandleRollbackRecord


class RecordReplayHandle:
    """A proxy object used for record/replay modes. What adapters call a
    'handle' is typically a native database connection, but should not be
    confused with the Connection protocol, which is a dbt-adapters concept."""

    def __init__(self, native_handle: Any, connection: Connection) -> None:
        self.native_handle = native_handle
        self.connection = connection

    def cursor(self) -> Any:
        # The native handle could be None if we are in replay mode, because no
        # actual database access should be performed in that mode.
        cursor = None if self.native_handle is None else self.native_handle.cursor()
        return RecordReplayCursor(cursor, self.connection)

    @property
    def connection_name(self) -> Optional[str]:
        return self.connection.name

    @record_function(HandleCommitRecord, method=True, id_field_name="connection_name")
    def commit(self):
        self.native_handle.commit()

    @record_function(HandleRollbackRecord, method=True, id_field_name="connection_name")
    def rollback(self):
        self.native_handle.rollback()

    @record_function(HandleCloseRecord, method=True, id_field_name="connection_name")
    def close(self):
        self.native_handle.close()

    @record_function(HandleGetBackendPidRecord, method=True, id_field_name="connection_name")
    def get_backend_pid(self):
        return self.native_handle.get_backend_pid()

    @property
    @record_function(HandleGetClosedRecord, method=True, id_field_name="connection_name")
    def closed(self):
        return self.native_handle.closed
