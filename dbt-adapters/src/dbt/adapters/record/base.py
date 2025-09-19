"""Implementations of record/replay classes for the base adapter implementation."""

import dataclasses

from typing import Optional, Tuple, Dict, Any, TYPE_CHECKING, List

from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.record.serialization import serialize_agate_table, serialize_bindings
from dbt_common.record import Record, Recorder

if TYPE_CHECKING:
    from agate import Table
    from dbt.adapters.base.relation import BaseRelation


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


@dataclasses.dataclass
class AdapterAddQueryParams:
    thread_id: str
    sql: str
    auto_begin: bool = True
    bindings: Optional[Any] = None
    abridge_sql_log: bool = False

    def _to_dict(self):
        return {
            "thread_id": self.thread_id,
            "sql": self.sql,
            "auto_begin": self.auto_begin,
            "bindings": serialize_bindings(self.bindings),
            "abridge_sql_log": self.abridge_sql_log,
        }


@dataclasses.dataclass
class AdapterAddQueryResult:
    return_val: Tuple[str, str]

    def _to_dict(self):
        return {
            "return_val": {
                "conn": "conn",
                "cursor": "cursor",
            }
        }


@Recorder.register_record_type
class AdapterAddQueryRecord(Record):
    params_cls = AdapterAddQueryParams
    result_cls = AdapterAddQueryResult
    group = "Available"


@dataclasses.dataclass
class AdapterListRelationsWithoutCachingParams:
    thread_id: str
    schema_relation: "BaseRelation"

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation

        return {
            "thread_id": self.thread_id,
            "schema_relation": serialize_base_relation(self.schema_relation),
        }

    def _from_dict(self, data: Dict[str, Any]):
        from dbt.adapters.record.serialization import deserialize_base_relation

        self.thread_id = data["thread_id"]
        self.schema_relation = deserialize_base_relation(data["schema_relation"])


@dataclasses.dataclass
class AdapterListRelationsWithoutCachingResult:
    return_val: List["BaseRelation"]

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation_list

        return {"return_val": serialize_base_relation_list(self.return_val)}

    def _from_dict(self, data: Dict[str, Any]):
        from dbt.adapters.record.serialization import deserialize_base_relation_list

        self.return_val = deserialize_base_relation_list(data["return_val"])


@Recorder.register_record_type
class AdapterListRelationsWithoutCachingRecord(Record):
    """Implements record/replay support for the BaseAdapter.list_relations_without_caching() method."""

    params_cls = AdapterListRelationsWithoutCachingParams
    result_cls = AdapterListRelationsWithoutCachingResult
    group = "Available"
