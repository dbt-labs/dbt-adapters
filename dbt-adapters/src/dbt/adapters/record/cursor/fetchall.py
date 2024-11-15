import dataclasses
import datetime
from typing import Any, Dict, List, Mapping

from dbt_common.record import Record, Recorder


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
    def _process_value(cls, value: Any) -> Any:
        if type(value) is datetime.date:
            return {"type": "date", "value": value.isoformat()}
        elif type(value) is datetime.datetime:
            return {"type": "datetime", "value": value.isoformat()}
        else:
            return value

    @classmethod
    def _unprocess_value(cls, value: Any) -> Any:
        if type(value) is dict:
            value_type = value.get("type")
            if value_type == "date":
                date_string = value.get("value")
                assert isinstance(date_string, str)
                return datetime.date.fromisoformat(date_string)
            elif value_type == "datetime":
                date_string = value.get("value")
                assert isinstance(date_string, str)
                return datetime.datetime.fromisoformat(date_string)
            return value
        else:
            return value


@Recorder.register_record_type
class CursorFetchAllRecord(Record):
    """Implements record/replay support for the cursor.fetchall() method."""

    params_cls = CursorFetchAllParams
    result_cls = CursorFetchAllResult
    group = "Database"
