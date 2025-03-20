import dataclasses

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class HandleCloseParams:
    connection_name: str

@Recorder.register_record_type
class HandleCloseRecord(Record):
    """Implements record/replay support for the handle.close property."""

    params_cls = HandleCloseParams
    result_cls = None
    group = "Database"
