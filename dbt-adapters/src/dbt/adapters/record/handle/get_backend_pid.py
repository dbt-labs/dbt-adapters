import dataclasses

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class HandleGetBackendPidParams:
    connection_name: str

@Recorder.register_record_type
class HandleGetBackendPidRecord(Record):
    """Implements record/replay support for the handle.commit() method."""

    params_cls = HandleGetBackendPidParams
    result_cls = None
    group = "Database"
