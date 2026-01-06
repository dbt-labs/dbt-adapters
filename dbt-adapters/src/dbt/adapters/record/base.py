"""Implementations of record/replay classes for the base adapter implementation."""

import dataclasses

from typing import Optional, Tuple, Dict, Any, TYPE_CHECKING, List

from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.record.serialization import serialize_agate_table, serialize_bindings
from dbt_common.record import Record, Recorder

if TYPE_CHECKING:
    from agate import Table
    from dbt.adapters.base.relation import BaseRelation
    from dbt.adapters.base.column import Column as BaseColumn


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

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "AdapterExecuteResult":
        """Deserialize AdapterExecuteResult from a dictionary for replay."""
        from dbt.adapters.record.serialization import deserialize_agate_table

        return_val_data = data.get("return_val", {})

        # Deserialize AdapterResponse
        adapter_response_data = return_val_data.get("adapter_response", {})
        adapter_response = AdapterResponse(
            _message=adapter_response_data.get("_message", ""),
            code=adapter_response_data.get("code"),
            rows_affected=adapter_response_data.get("rows_affected"),
            query_id=adapter_response_data.get("query_id"),
        )

        # Deserialize agate Table
        table_data = return_val_data.get("table", {"column_names": [], "column_types": [], "rows": []})
        table = deserialize_agate_table(table_data)

        return cls(return_val=(adapter_response, table))


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

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "AdapterGetPartitionsMetadataResult":
        """Deserialize AdapterGetPartitionsMetadataResult from a dictionary for replay."""
        from dbt.adapters.record.serialization import deserialize_agate_table

        # data is expected to be a list of serialized tables
        tables = tuple(deserialize_agate_table(table_data) for table_data in data)
        return cls(return_val=tables)


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

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "AdapterConvertTypeParams":
        """Deserialize AdapterConvertTypeParams from a dictionary for replay."""
        from dbt.adapters.record.serialization import deserialize_agate_table

        return cls(
            thread_id=data["thread_id"],
            table=deserialize_agate_table(data["table"]),
            col_idx=data["col_idx"],
        )


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

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "AdapterStandardizeGrantsDictParams":
        """Deserialize AdapterStandardizeGrantsDictParams from a dictionary for replay."""
        from dbt.adapters.record.serialization import deserialize_agate_table

        return cls(
            thread_id=data["thread_id"],
            table=deserialize_agate_table(data["table"]),
        )


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

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "AdapterListRelationsWithoutCachingParams":
        from dbt.adapters.record.serialization import deserialize_base_relation

        return cls(
            thread_id=data["thread_id"],
            schema_relation=deserialize_base_relation(data["schema_relation"]),
        )

    def __eq__(self, other):
        """Custom equality check that compares relations by their dict representation.

        This is needed because during replay, the recorded relation may be a BaseRelation
        while the actual relation is a subclass (e.g., SnowflakeRelation). We compare
        by dict to avoid type mismatch issues.
        """
        if not isinstance(other, AdapterListRelationsWithoutCachingParams):
            return False
        if self.thread_id != other.thread_id:
            return False
        # Compare relations by their dict representation, ignoring type differences
        self_dict = self.schema_relation.to_dict(omit_none=True)
        other_dict = other.schema_relation.to_dict(omit_none=True)

        # Normalize renameable_relations and replaceable_relations to sets of strings
        # because they may be stored as strings in recording but as enums at runtime
        def normalize_relation_types(d):
            d = dict(d)  # Make a copy
            for key in ['renameable_relations', 'replaceable_relations']:
                if key in d:
                    d[key] = set(str(v.value if hasattr(v, 'value') else v) for v in d[key])
            return d

        return normalize_relation_types(self_dict) == normalize_relation_types(other_dict)


@dataclasses.dataclass
class AdapterListRelationsWithoutCachingResult:
    return_val: List["BaseRelation"]

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation_list

        return {"return_val": serialize_base_relation_list(self.return_val)}

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "AdapterListRelationsWithoutCachingResult":
        from dbt.adapters.record.serialization import deserialize_base_relation_list

        return cls(return_val=deserialize_base_relation_list(data["return_val"]))


@Recorder.register_record_type
class AdapterListRelationsWithoutCachingRecord(Record):
    """Implements record/replay support for the BaseAdapter.list_relations_without_caching() method."""

    params_cls = AdapterListRelationsWithoutCachingParams
    result_cls = AdapterListRelationsWithoutCachingResult
    group = "Available"


@dataclasses.dataclass
class AdapterGetColumnsInRelationParams:
    thread_id: str
    relation: "BaseRelation"

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation

        return {
            "thread_id": self.thread_id,
            "relation": serialize_base_relation(self.relation),
        }

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "AdapterGetColumnsInRelationParams":
        from dbt.adapters.record.serialization import deserialize_base_relation

        return cls(
            thread_id=data["thread_id"],
            relation=deserialize_base_relation(data["relation"]),
        )


@dataclasses.dataclass
class AdapterGetColumnsInRelationResult:
    return_val: List["BaseColumn"]

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_column_list

        return {"return_val": serialize_base_column_list(self.return_val)}

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "AdapterGetColumnsInRelationResult":
        from dbt.adapters.record.serialization import deserialize_base_column_list

        return cls(return_val=deserialize_base_column_list(data["return_val"]))


@Recorder.register_record_type
class AdapterGetColumnsInRelationRecord(Record):
    """Implements record/replay support for the BaseAdapter.get_columns_in_relation() method."""

    params_cls = AdapterGetColumnsInRelationParams
    result_cls = AdapterGetColumnsInRelationResult
    group = "Available"
