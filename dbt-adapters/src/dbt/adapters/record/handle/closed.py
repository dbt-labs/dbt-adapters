import dataclasses

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class HandleGetClosedParams:
    connection_name: str


@dataclasses.dataclass
class HandleGetClosedResult:
    closed: bool


@Recorder.register_record_type
class HandleGetClosedRecord(Record):
    """Implements record/replay support for the handle.closed property."""

    params_cls = HandleGetClosedParams
    result_cls = HandleGetClosedResult
    group = "Database"
