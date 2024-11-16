import dataclasses
from typing import Any

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class CursorFetchOneParams:
    connection_name: str


@dataclasses.dataclass
class CursorFetchOneResult:
    result: Any


@Recorder.register_record_type
class CursorFetchOneRecord(Record):
    """Implements record/replay support for the cursor.fetchone() method."""

    params_cls = CursorFetchOneParams
    result_cls = CursorFetchOneResult
    group = "Database"
