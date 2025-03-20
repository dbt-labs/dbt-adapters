import dataclasses

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class HandleCommitParams:
    connection_name: str

@Recorder.register_record_type
class HandleCommitRecord(Record):
    """Implements record/replay support for the handle.commit() method."""

    params_cls = HandleCommitParams
    result_cls = None
    group = "Database"
