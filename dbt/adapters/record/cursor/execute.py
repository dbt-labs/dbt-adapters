import dataclasses
from typing import Any, Iterable, Union, Mapping

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class CursorExecuteParams:
    connection_name: str
    operation: str
    parameters: Union[Iterable[Any], Mapping[str, Any]]


@Recorder.register_record_type
class CursorExecuteRecord(Record):
    """Implements record/replay support for the cursor.execute() method."""

    params_cls = CursorExecuteParams
    result_cls = None
    group = "Database"
