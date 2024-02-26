import dataclasses
from io import StringIO
import json
import re
from typing import Any, Optional, Mapping, Tuple

from agate import Table

from dbt_common.events.contextvars import get_node_info
from dbt_common.record import Record, Recorder

from dbt.adapters.contracts.connection import AdapterResponse


@dataclasses.dataclass
class QueryRecordParams:
    sql: str
    auto_begin: bool
    fetch: bool
    limit: Optional[int]
    node_unique_id: str

    def __init__(self, obj: Any, sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None):
        self.sql = sql
        self.auto_begin = auto_begin
        self.fetch = fetch
        self.limit = limit
        node_info = get_node_info()
        self.node_unique_id = node_info["unique_id"] if node_info else ""

    @staticmethod
    def _clean_up_sql(sql: str) -> str:
        sql = re.sub(r"--.*?\n", "", sql)  # Remove single-line comments (--)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)  # Remove multi-line comments (/* */)
        return sql.replace(" ", "").replace("\n", "")

    def matches(self, other: "QueryRecordParams") -> bool:
        return self.node_unique_id == other.node_unique_id and self._clean_up_sql(self.sql) == self._clean_up_sql(other.sql)


@dataclasses.dataclass
class QueryRecordResult:
    adapter_response: Optional["AdapterResponse"]
    table: Optional[Table]

    def __init__(self, ret_val: Tuple[AdapterResponse, Table]):
        self.adapter_response = ret_val[0]
        self.table = ret_val[1]

    def to_dict(self) -> Any:
        buf = StringIO()
        self.table.to_json(buf)  # type: ignore

        return {
            "adapter_response": self.adapter_response.to_dict(),  # type: ignore
            "table": buf.getvalue(),
        }

    @classmethod
    def from_dict(cls, dct: Mapping) -> "QueryRecordResult":
        return QueryRecordResult(
            adapter_response=AdapterResponse.from_dict(dct["adapter_response"]),
            table=Table.from_object(json.loads(dct["table"])),
        )


class QueryRecord(Record):
    params_cls = QueryRecordParams
    result_cls = QueryRecordResult


Recorder.register_record_type(QueryRecord)
