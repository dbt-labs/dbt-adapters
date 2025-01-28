import dataclasses
from typing import Optional

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class CursorGetRowCountParams:
    connection_name: str


@dataclasses.dataclass
class CursorGetRowCountResult:
    rowcount: Optional[int]


@Recorder.register_record_type
class CursorGetRowCountRecord(Record):
    """Implements record/replay support for the cursor.rowcount property."""

    params_cls = CursorGetRowCountParams
    result_cls = CursorGetRowCountResult
    group = "Database"
