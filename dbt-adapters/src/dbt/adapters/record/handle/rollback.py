import dataclasses

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class HandleRollbackParams:
    connection_name: str

@Recorder.register_record_type
class HandleRollbackRecord(Record):
    """Implements record/replay support for the handle.rollback() method."""

    params_cls = HandleRollbackParams
    result_cls = None
    group = "Database"
