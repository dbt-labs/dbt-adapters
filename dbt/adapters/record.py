import dataclasses
from io import StringIO
import json
import re
from typing import Any, Optional, Mapping

from agate import Table

from dbt_common.events.contextvars import get_node_info
from dbt_common.record import Record, Recorder

from dbt.adapters.contracts.connection import AdapterResponse


@dataclasses.dataclass
class QueryRecordParams:
    sql: str
    auto_begin: bool = False
    fetch: bool = False
    limit: Optional[int] = None
    node_unique_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.node_unique_id is None:
            node_info = get_node_info()
            self.node_unique_id = node_info["unique_id"] if node_info else ""

    @staticmethod
    def _clean_up_sql(sql: str) -> str:
        sql = re.sub(r"--.*?\n", "", sql)  # Remove single-line comments (--)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)  # Remove multi-line comments (/* */)
        return sql.replace(" ", "").replace("\n", "")

    def _matches(self, other: "QueryRecordParams") -> bool:
        return self.node_unique_id == other.node_unique_id and self._clean_up_sql(
            self.sql
        ) == self._clean_up_sql(other.sql)


@dataclasses.dataclass
class QueryRecordResult:
    adapter_response: Optional["AdapterResponse"]
    table: Optional[Table]

    def _to_dict(self) -> Any:
        buf = StringIO()
        self.table.to_json(buf)  # type: ignore

        return {
            "adapter_response": self.adapter_response.to_dict(),  # type: ignore
            "table": buf.getvalue(),
        }

    @classmethod
    def _from_dict(cls, dct: Mapping) -> "QueryRecordResult":
        return QueryRecordResult(
            adapter_response=AdapterResponse.from_dict(dct["adapter_response"]),
            table=Table.from_object(json.loads(dct["table"])),
        )


class QueryRecord(Record):
    params_cls = QueryRecordParams
    result_cls = QueryRecordResult


Recorder.register_record_type(QueryRecord)
