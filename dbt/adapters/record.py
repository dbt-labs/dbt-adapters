import dataclasses
import datetime
from typing import Any, Dict, Optional, Mapping, List, Union, Iterable

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


@Recorder.register_record_type
class CursorExecuteRecord(Record):
    params_cls = CursorExecuteParams
    result_cls = None
    group = "Database"


@dataclasses.dataclass
class CursorFetchOneParams:
    connection_name: str


@dataclasses.dataclass
class CursorFetchOneResult:
    result: Any


@Recorder.register_record_type
class CursorFetchOneRecord(Record):
    params_cls = CursorFetchOneParams
    result_cls = CursorFetchOneResult
    group = "Database"


@dataclasses.dataclass
class CursorFetchManyParams:
    connection_name: str


@dataclasses.dataclass
class CursorFetchManyResult:
    results: List[Any]


@Recorder.register_record_type
class CursorFetchManyRecord(Record):
    params_cls = CursorFetchManyParams
    result_cls = CursorFetchManyResult
    group = "Database"


@dataclasses.dataclass
class CursorFetchAllParams:
    connection_name: str


@dataclasses.dataclass
class CursorFetchAllResult:
    results: List[Any]

    def _to_dict(self) -> Dict[str, Any]:
        processed_results = []
        for result in self.results:
            result = tuple(map(self._process_value, result))
            processed_results.append(result)

        return {"results": processed_results}

    @classmethod
    def _from_dict(cls, dct: Mapping) -> "CursorFetchAllResult":
        unprocessed_results = []
        for result in dct["results"]:
            result = tuple(map(cls._unprocess_value, result))
            unprocessed_results.append(result)

        return CursorFetchAllResult(unprocessed_results)

    @classmethod
    def _process_value(self, value: Any) -> Any:
        if type(value) is datetime.date:
            return {"type": "date", "value": value.isoformat()}
        elif type(value) is datetime.datetime:
            return {"type": "datetime", "value": value.isoformat()}
        else:
            return value

    @classmethod
    def _unprocess_value(self, value: Any) -> Any:
        if type(value) is dict:
            value_type = value.get("type")
            if value_type == "date":
                return datetime.date.fromisoformat(value.get("value"))
            elif value_type == "datetime":
                return datetime.datetime.fromisoformat(value.get("value"))
            return value
        else:
            return value


@Recorder.register_record_type
class CursorFetchAllRecord(Record):
    params_cls = CursorFetchAllParams
    result_cls = CursorFetchAllResult
    group = "Database"


@dataclasses.dataclass
class CursorGetRowCountParams:
    connection_name: str


@dataclasses.dataclass
class CursorGetRowCountResult:
    rowcount: Optional[int]


@Recorder.register_record_type
class CursorGetRowCountRecord(Record):
    params_cls = CursorGetRowCountParams
    result_cls = CursorGetRowCountResult
    group = "Database"


@dataclasses.dataclass
class CursorGetDescriptionParams:
    connection_name: str


@dataclasses.dataclass
class CursorGetDescriptionResult:
    columns: Iterable[Any]

    def _to_dict(self) -> Any:
        column_dicts = []
        for c in self.columns:
            # This captures the mandatory column information, but we might need
            # more for some adapters.
            # See https://peps.python.org/pep-0249/#description
            column_dicts.append((c[0], c[1]))

        return {"columns": column_dicts}

    @classmethod
    def _from_dict(cls, dct: Mapping) -> "CursorGetDescriptionResult":
        return CursorGetDescriptionResult(columns=dct["columns"])


@Recorder.register_record_type
class CursorGetDescriptionRecord(Record):
    params_cls = CursorGetDescriptionParams
    result_cls = CursorGetDescriptionResult
    group = "Database"


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
