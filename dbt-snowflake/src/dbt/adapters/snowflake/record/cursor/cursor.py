from dbt_common.record import record_function

from dbt.adapters.record import RecordReplayCursor
from dbt.adapters.snowflake.record.cursor.sfqid import CursorGetSfqidRecord
from dbt.adapters.snowflake.record.cursor.sqlstate import CursorGetSqlStateRecord
from dbt.adapters.snowflake.record.cursor.stats import CursorGetStatsRecord


class SnowflakeRecordReplayCursor(RecordReplayCursor):
    """A custom extension of RecordReplayCursor that adds the sqlstate,
    sfqid, and stats properties which are specific to snowflake-connector."""

    @property
    @record_function(CursorGetSqlStateRecord, method=True, id_field_name="connection_name")
    def sqlstate(self):
        return self.native_cursor.sqlstate

    @property
    @record_function(CursorGetSfqidRecord, method=True, id_field_name="connection_name")
    def sfqid(self):
        return self.native_cursor.sfqid

    @property
    @record_function(CursorGetStatsRecord, method=True, id_field_name="connection_name")
    def stats(self):
        return self.native_cursor.stats
