import dataclasses
from typing import Any, List

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class CursorFetchManyParams:
    connection_name: str


@dataclasses.dataclass
class CursorFetchManyResult:
    results: List[Any]


@Recorder.register_record_type
class CursorFetchManyRecord(Record):
    """Implements record/replay support for the cursor.fetchmany() method."""

    params_cls = CursorFetchManyParams
    result_cls = CursorFetchManyResult
    group = "Database"
