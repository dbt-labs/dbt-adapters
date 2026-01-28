import dataclasses
from typing import Optional

from dbt_common.record import Record, Recorder, record_function

from dbt.adapters.record import RecordReplayHandle
from dbt.adapters.snowflake.record.cursor.cursor import SnowflakeRecordReplayCursor


@dataclasses.dataclass
class HandleGetSessionIdParams:
    connection_name: str


@dataclasses.dataclass
class HandleGetSessionIdResult:
    session_id: Optional[int]


@Recorder.register_record_type
class HandleGetSessionIdRecord(Record):
    params_cls = HandleGetSessionIdParams
    result_cls = HandleGetSessionIdResult
    group = "Database"


class SnowflakeRecordReplayHandle(RecordReplayHandle):
    """A custom extension of RecordReplayHandle that returns a
    snowflake-connector-specific SnowflakeRecordReplayCursor object and adds
    the session_id property which is specific to snowflake-connector."""

    def cursor(self):
        cursor = None if self.native_handle is None else self.native_handle.cursor()
        return SnowflakeRecordReplayCursor(cursor, self.connection)

    @property
    def connection_name(self) -> Optional[str]:
        return self.connection.name

    @property
    @record_function(HandleGetSessionIdRecord, method=True, id_field_name="connection_name")
    def session_id(self):
        return self.native_handle.session_id
