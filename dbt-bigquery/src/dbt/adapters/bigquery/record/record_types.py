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
    return_val: "BigQueryTable"

    def _to_dict(self):
        return {
            "return_val": self.return_val.to_dict(),
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
    relation: "BigQueryRelation"
    conf_partition: Optional["PartitionConfig"]
    conf_cluster: Optional[Union[List[str], str]]

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation

        return {
            "thread_id": self.thread_id,
            "relation": serialize_base_relation(self.relation),
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