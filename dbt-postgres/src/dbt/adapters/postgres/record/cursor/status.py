import dataclasses
from typing import Optional

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class CursorGetStatusMessageParams:
    connection_name: str


@dataclasses.dataclass
class CursorGetStatusMessageResult:
    msg: Optional[str]


@Recorder.register_record_type
class CursorGetStatusMessageRecord(Record):
    params_cls = CursorGetStatusMessageParams
    result_cls = CursorGetStatusMessageResult
    group = "Database"
