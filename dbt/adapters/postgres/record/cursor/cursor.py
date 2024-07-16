from dbt_common.record import record_function

from dbt.adapters.record import RecordReplayCursor

from dbt.adapters.postgres.record.cursor.status import CursorGetStatusMessageRecord


class PostgresRecordReplayCursor(RecordReplayCursor):
    """A custom extension of RecordReplayCursor that adds the statusmessage
    property which is specific to psycopg."""

    @property
    @record_function(CursorGetStatusMessageRecord, method=True, id_field_name="connection_name")
    def statusmessage(self):
        return self.native_cursor.statusmessage
