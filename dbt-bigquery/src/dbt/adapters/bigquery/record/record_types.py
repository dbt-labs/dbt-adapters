import dataclasses
from typing import Optional, List, Union

from google.cloud.bigquery import Table as BigQueryTable

from dbt_common.record import Record, Recorder
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.relation_configs import PartitionConfig


@dataclasses.dataclass
class BigQueryAdapterGetBqTableParams:
    thread_id: str
    relation: "BigQueryRelation"

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation

        return {
            "thread_id": self.thread_id,
            "relation": serialize_base_relation(self.relation),
        }


@dataclasses.dataclass
class BigQueryAdapterGetBqTableResult:
    return_val: Optional["BigQueryTable"]

    def _to_dict(self):
        return {
            "return_val": self.return_val.to_dict() if self.return_val else None,
        }


@Recorder.register_record_type
class BigQueryAdapterGetBqTableRecord(Record):
    """Implements record/replay support for the BigQueryAdapter.get_bq_table() method."""

    params_cls = BigQueryAdapterGetBqTableParams
    result_cls = BigQueryAdapterGetBqTableResult
    group = "Available"


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
