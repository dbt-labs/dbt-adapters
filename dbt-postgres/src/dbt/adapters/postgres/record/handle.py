from dbt.adapters.record import RecordReplayHandle

from dbt.adapters.postgres.record.cursor.cursor import PostgresRecordReplayCursor


class PostgresRecordReplayHandle(RecordReplayHandle):
    """A custom extension of RecordReplayHandle that returns
    a psycopg-specific PostgresRecordReplayCursor object."""

    def cursor(self):
        cursor = None if self.native_handle is None else self.native_handle.cursor()
        return PostgresRecordReplayCursor(cursor, self.connection)
