from dataclasses import dataclass
from typing import Optional

from dbt.adapters.contracts.connection import AdapterResponse


@dataclass
class SnowflakeAdapterResponse(AdapterResponse):
    """Extended AdapterResponse for Snowflake that includes DML statistics.

    These stats are populated from SnowflakeCursor.stats (available in snowflake-connector-python >= 4.2.0)
    and provide granular information about DML operations like CTAS, INSERT, UPDATE, DELETE.
    """

    rows_inserted: Optional[int] = None
    rows_deleted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_duplicates: Optional[int] = None
