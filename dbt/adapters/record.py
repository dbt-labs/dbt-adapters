import dataclasses
from typing import Any, Optional, Mapping, List, Union, Iterable

from dbt.adapters.contracts.connection import Connection

from dbt_common.record import Record, Recorder, record_function


class RecordReplayHandle:
    def __init__(self, native_handle: Any, connection: Connection) -> None:
        self.native_handle = native_handle
        self.connection = connection

    def cursor(self):
        # The native handle could be None if we are in replay mode, because no
        # actual database access should be performed in that mode.
        cursor = None if self.native_handle is None else self.native_handle.cursor()
        return RecordReplayCursor(cursor, self.connection)


@dataclasses.dataclass
class CursorExecuteParams:
    connection_name: str
    operation: str
    parameters: Union[Iterable[Any], Mapping[str, Any]]


class CursorExecuteRecord(Record):
    params_cls = CursorExecuteParams
    result_cls = None


Recorder.register_record_type(CursorExecuteRecord)


@dataclasses.dataclass
class CursorFetchOneParams:
    connection_name: str


@dataclasses.dataclass
class CursorFetchOneResult:
    result: Any


class CursorFetchOneRecord(Record):
    params_cls = CursorFetchOneParams
    result_cls = CursorFetchOneResult


Recorder.register_record_type(CursorFetchOneRecord)


@dataclasses.dataclass
class CursorFetchManyParams:
    connection_name: str


@dataclasses.dataclass
class CursorFetchManyResult:
    results: List[Any]


class CursorFetchManyRecord(Record):
    params_cls = CursorFetchManyParams
    result_cls = CursorFetchManyResult


Recorder.register_record_type(CursorFetchManyRecord)


@dataclasses.dataclass
class CursorFetchAllParams:
    connection_name: str


@dataclasses.dataclass
class CursorFetchAllResult:
    results: List[Any]


class CursorFetchAllRecord(Record):
    params_cls = CursorFetchAllParams
    result_cls = CursorFetchAllResult


Recorder.register_record_type(CursorFetchAllRecord)


@dataclasses.dataclass
class CursorGetRowCountParams:
    connection_name: str


@dataclasses.dataclass
class CursorGetRowCountResult:
    rowcount: Optional[int]


class CursorGetRowCountRecord(Record):
    params_cls = CursorGetRowCountParams
    result_cls = CursorGetRowCountResult


Recorder.register_record_type(CursorGetRowCountRecord)


@dataclasses.dataclass
class CursorGetDescriptionParams:
    connection_name: str


@dataclasses.dataclass
class RecordReplayColumn:
    name: str
    type_code: int
    display_size: Optional[int]
    internal_size: int
    null_ok: Optional[bool]
    precision: Optional[int]
    scale: Optional[int]
    table_column: Optional[int]
    table_oid: Optional[int]


@dataclasses.dataclass
class CursorGetDescriptionResult:
    columns: Iterable[Any]

    def _to_dict(self) -> Any:
        column_dicts = []

        for c in self.columns:
            column_dicts.append(
                {
                    "name": c.name,
                    "type_code": c.type_code,
                    "display_size": c.display_size,
                    "internal_size": c.internal_size,
                    "null_ok": c.null_ok,
                    "precision": c.precision,
                    "scale": c.scale,
                    "table_column": c.table_column,
                    "table_oid": c.table_oid,
                }
            )

        return {"columns": column_dicts}

    @classmethod
    def _from_dict(cls, dct: Mapping) -> "CursorGetDescriptionResult":
        columns = iter(
            RecordReplayColumn(
                c["name"],
                c["type_code"],
                c["display_size"],
                c["internal_size"],
                c["null_ok"],
                c["precision"],
                c["scale"],
                c["table_column"],
                c["table_oid"],
            )
            for c in dct["columns"]
        )
        return CursorGetDescriptionResult(tuple(columns))


class CursorGetDescriptionRecord(Record):
    params_cls = CursorGetDescriptionParams
    result_cls = CursorGetDescriptionResult


Recorder.register_record_type(CursorGetDescriptionRecord)


class RecordReplayCursor:
    def __init__(self, native_cursor: Any, connection: Connection) -> None:
        self.native_cursor = native_cursor
        self.connection = connection

    @record_function(CursorExecuteRecord, method=True, id_field_name="connection_name")
    def execute(self, operation, parameters=None) -> None:
        self.native_cursor.execute(operation, parameters)

    @record_function(CursorFetchOneRecord, method=True, id_field_name="connection_name")
    def fetchone(self) -> Any:
        return self.native_cursor.fetchone()

    @record_function(CursorFetchManyRecord, method=True, id_field_name="connection_name")
    def fetchmany(self, size: int) -> Any:
        return self.native_cursor.fetchmany(size)

    @record_function(CursorFetchAllRecord, method=True, id_field_name="connection_name")
    def fetchall(self) -> Any:
        return self.native_cursor.fetchall()

    @property
    def connection_name(self) -> Optional[str]:
        return self.connection.name

    @property
    @record_function(CursorGetRowCountRecord, method=True, id_field_name="connection_name")
    def rowcount(self) -> int:
        return self.native_cursor.rowcount

    @property
    @record_function(CursorGetDescriptionRecord, method=True, id_field_name="connection_name")
    def description(self) -> str:
        return self.native_cursor.description
