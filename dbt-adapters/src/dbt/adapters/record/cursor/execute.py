import dataclasses
from typing import Any, Iterable, Union, Mapping, Optional

from dbt.adapters.record.cursor.fetchall import CursorFetchAllResult
from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class CursorExecuteParams:
    connection_name: str
    operation: str
    parameters: Optional[Union[Iterable[Any], Mapping[str, Any]]] = None

    def _to_dict(self):
        p = self.parameters
        if isinstance(self.parameters, dict):
            p = {(k, CursorFetchAllResult._process_value(v)) for k, v in self.parameters.items()}
        elif isinstance(self.parameters, list) or isinstance(self.parameters, tuple):
            p = [CursorFetchAllResult._process_value(v) for v in self.parameters]

        return {
            "connection_name": self.connection_name,
            "operation": self.operation,
            "parameters": p,
        }

    def _from_dict(cls, data):
        # NOTE: This will be needed for replay, but is not needed at time
        # of writing.
        raise NotImplementedError()


@Recorder.register_record_type
class CursorExecuteRecord(Record):
    """Implements record/replay support for the cursor.execute() method."""

    params_cls = CursorExecuteParams
    result_cls = None
    group = "Database"
