import dataclasses
from typing import Dict, Optional, List, Union

from dbt_common.record import Record, Recorder
from dbt.adapters.bigquery.column import BigQueryColumn
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.relation_configs import PartitionConfig


@dataclasses.dataclass
class BigQueryAdapterIsReplaceableParams:
    thread_id: str
    relation: Optional["BigQueryRelation"]
    conf_partition: Optional["PartitionConfig"]
    conf_cluster: Optional[Union[List[str], str]]

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation

        return {
            "thread_id": self.thread_id,
            "relation": serialize_base_relation(self.relation) if self.relation else None,
            "conf_partition": self.conf_partition.to_dict() if self.conf_partition else None,
            "conf_cluster": self.conf_cluster,
        }


@dataclasses.dataclass
class BigQueryAdapterIsReplaceableResult:
    return_val: bool


@Recorder.register_record_type
class BigQueryAdapterIsReplaceableRecord(Record):
    """Implements record/replay support for the BigQueryAdapter.is_replaceable() method."""

    params_cls = BigQueryAdapterIsReplaceableParams
    result_cls = BigQueryAdapterIsReplaceableResult
    group = "Available"


@dataclasses.dataclass
class BigQueryAdapterDescribeRelationParams:
    thread_id: str
    relation: "BigQueryRelation"

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation

        return {
            "thread_id": self.thread_id,
            "relation": serialize_base_relation(self.relation),
        }


@dataclasses.dataclass
class BigQueryAdapterDescribeRelationResult:
    return_val: Optional[dict]  # BigQueryBaseRelationConfig serialized as dict, or None

    def _to_dict(self):
        # return_val is already converted to dict by the constructor
        return {
            "return_val": self.return_val,
        }

    def __init__(self, return_val):
        # Handle BigQueryBaseRelationConfig by converting to dict
        if return_val is not None and not isinstance(return_val, dict):
            self.return_val = dataclasses.asdict(return_val)
        else:
            self.return_val = return_val

    @classmethod
    def _from_dict(cls, data):
        return cls(return_val=data.get("return_val"))


@Recorder.register_record_type
class BigQueryAdapterDescribeRelationRecord(Record):
    """Implements record/replay support for the BigQueryAdapter.describe_relation() method."""

    params_cls = BigQueryAdapterDescribeRelationParams
    result_cls = BigQueryAdapterDescribeRelationResult
    group = "Available"


@dataclasses.dataclass
class BigQueryAdapterCopyTableParams:
    thread_id: str
    source: Union[BigQueryRelation, List[BigQueryRelation]]
    destination: BigQueryRelation
    materialization: str

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation

        source_array = [self.source] if type(self.source) is not list else self.source

        return {
            "thread_id": self.thread_id,
            "source": [serialize_base_relation(source) for source in source_array],
            "destination": serialize_base_relation(self.destination),
            "materialization": self.materialization
        }

@dataclasses.dataclass
class BigQueryAdapterCopyTableResult:
    return_val: str

@Recorder.register_record_type
class BigQueryAdapterCopyTableRecord(Record):
    """Implements record/replay support for the BigQueryAdapter.copy_table() method."""

    params_cls = BigQueryAdapterCopyTableParams
    result_cls = BigQueryAdapterCopyTableResult
    group = "Available"


@dataclasses.dataclass
class BigQueryAdapterGetDatasetLocationParams:
    thread_id: str
    relation: BigQueryRelation

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation

        return {
            "thread_id": self.thread_id,
            "relation": serialize_base_relation(self.relation),
        }


@dataclasses.dataclass
class BigQueryAdapterGetDatasetLocationResult:
    return_val: str


@Recorder.register_record_type
class BigQueryAdapterGetDatasetLocationRecord(Record):
    """Implements record/replay support for the BigQueryAdapter.get_dataset_location() method."""

    params_cls = BigQueryAdapterGetDatasetLocationParams
    result_cls = BigQueryAdapterGetDatasetLocationResult
    group = "Available"


@dataclasses.dataclass
class BigQueryAdapterGrantAccessToParams:
    thread_id: str
    entity: BigQueryRelation
    entity_type: str
    role: Optional[str]
    grant_target_dict: Dict[str, str]

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation

        return {
            "thread_id": self.thread_id,
            "entity": serialize_base_relation(self.entity),
            "entity_type": self.entity_type,
            "role": self.role,
            "grant_target_dict": self.grant_target_dict,
        }


@Recorder.register_record_type
class BigQueryAdapterGrantAccessToRecord(Record):
    """Implements record/replay support for the BigQueryAdapter.grant_access_to() method."""

    params_cls = BigQueryAdapterGrantAccessToParams
    result_cls = None
    group = "Available"


@dataclasses.dataclass
class BigQueryAdapterGetColumnsInSelectSqlParams:
    thread_id: str
    select_sql: str


@dataclasses.dataclass
class BigQueryAdapterGetColumnsInSelectSqlResult:
    return_val: List[BigQueryColumn]

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_column_list

        return {"return_val": serialize_base_column_list(self.return_val)}

    def _from_dict(self, data):
        from dbt.adapters.record.serialization import deserialize_base_column_list

        self.return_val = deserialize_base_column_list(data["return_val"])


@Recorder.register_record_type
class BigQueryAdapterGetColumnsInSelectSqlRecord(Record):
    """Implements record/replay support for the BigQueryAdapter.get_columns_in_select_sql() method."""

    params_cls = BigQueryAdapterGetColumnsInSelectSqlParams
    result_cls = BigQueryAdapterGetColumnsInSelectSqlResult
    group = "Available"


@dataclasses.dataclass
class BigQueryAdapterAlterTableAddColumnsParams:
    thread_id: str
    relation: BigQueryRelation
    columns: List[BigQueryColumn]

    def _to_dict(self):
        from dbt.adapters.record.serialization import (
            serialize_base_relation,
            serialize_base_column_list,
        )

        return {
            "thread_id": self.thread_id,
            "relation": serialize_base_relation(self.relation),
            "columns": serialize_base_column_list(self.columns),
        }


@Recorder.register_record_type
class BigQueryAdapterAlterTableAddColumnsRecord(Record):
    """Implements record/replay support for the BigQueryAdapter.alter_table_add_columns() method."""

    params_cls = BigQueryAdapterAlterTableAddColumnsParams
    result_cls = None
    group = "Available"
