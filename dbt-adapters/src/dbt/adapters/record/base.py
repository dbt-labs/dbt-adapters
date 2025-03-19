"""Implementations of record/replay classes for the base adapter implementation."""

import dataclasses

from typing import Optional, Tuple, Dict, Any, TYPE_CHECKING

from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.record.serialization import serialize_agate_table
from dbt_common.record import Record, Recorder

if TYPE_CHECKING:
    from agate import Table


@dataclasses.dataclass
class AdapterExecuteParams:
    thread_id: str
    sql: str
    auto_begin: bool = False
    fetch: bool = False
    limit: Optional[int] = None


@dataclasses.dataclass
class AdapterExecuteResult:
    return_val: Tuple[AdapterResponse, "Table"]

    def _to_dict(self):
        adapter_response = self.return_val[0]
        table = self.return_val[1]
        return {
            "return_val": {
                "adapter_response": adapter_response.to_dict(),
                "table": serialize_agate_table(table),
            }
        }

    def _from_dict(self, data: Dict[str, Any]):
        # We will need this for replay, but it is not a priority at time of writing.
        raise NotImplementedError()


@Recorder.register_record_type
class AdapterExecuteRecord(Record):
    """Implements record/replay support for the BaseAdapter.execute() method."""

    params_cls = AdapterExecuteParams
    result_cls = AdapterExecuteResult
    group = "Available"


@dataclasses.dataclass
class AdapterTestSqlResult:
    return_val: str


@dataclasses.dataclass
class AdapterTestSqlParams:
    thread_id: str
    sql: str
    fetch: str
    conn: Any

    def _to_dict(self):
        return {
            "thread_id": self.thread_id,
            "sql": self.sql,
            "fetch": self.fetch,
            "conn": "conn",
        }


@Recorder.register_record_type
class AdapterTestSqlRecord(Record):
    """Implements record/replay support for the BaseAdapter.execute() method."""

    params_cls = AdapterTestSqlParams
    result_cls = AdapterTestSqlResult
    group = "Available"


@dataclasses.dataclass
class AdapterGetPartitionsMetadataParams:
    thread_id: str
    table: str


@dataclasses.dataclass
class AdapterGetPartitionsMetadataResult:
    return_val: tuple["Table"]

    def _to_dict(self):
        return list(map(serialize_agate_table, self.return_val))

    def _from_dict(self, data: Dict[str, Any]):
        # We will need this for replay, but it is not a priority at time of writing.
        raise NotImplementedError()


@Recorder.register_record_type
class AdapterGetPartitionsMetadataRecord(Record):
    """Implements record/replay support for the BaseAdapter.get_partitions_metadata() method."""

    params_cls = AdapterGetPartitionsMetadataParams
    result_cls = AdapterGetPartitionsMetadataResult
    group = "Available"


@dataclasses.dataclass
class AdapterConvertTypeParams:
    thread_id: str
    table: "Table"
    col_idx: int

    def _to_dict(self):
        return {
            "thread_id": self.thread_id,
            "table": serialize_agate_table(self.table),
            "col_idx": self.col_idx,
        }

    def _from_dict(self, data: Dict[str, Any]):
        # We will need this for replay, but it is not a priority at time of writing.
        raise NotImplementedError()


@dataclasses.dataclass
class AdapterConvertTypeResult:
    return_val: Optional[str]


@Recorder.register_record_type
class AdapterConvertTypeRecord(Record):
    """Implements record/replay support for the BaseAdapter.convert_type() method."""

    params_cls = AdapterConvertTypeParams
    result_cls = AdapterConvertTypeResult
    group = "Available"


@dataclasses.dataclass
class AdapterStandardizeGrantsDictParams:
    thread_id: str
    table: "Table"

    def _to_dict(self):
        return {"thread_id": self.thread_id, "table": serialize_agate_table(self.table)}

    def _from_dict(self, data: Dict[str, Any]):
        # We will need this for replay, but it is not a priority at time of writing.
        raise NotImplementedError()


@dataclasses.dataclass
class AdapterStandardizeGrantsDictResult:
    return_val: dict


@Recorder.register_record_type
class AdapterStandardizeGrantsDictRecord(Record):
    params_cls = AdapterStandardizeGrantsDictParams
    result_cls = AdapterStandardizeGrantsDictResult
    group = "Available"
