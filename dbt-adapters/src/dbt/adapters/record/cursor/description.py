import dataclasses
from typing import Any, Iterable, Mapping

from dbt_common.record import Record, Recorder


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
    """Implements record/replay support for the cursor.description property."""

    params_cls = CursorGetDescriptionParams
    result_cls = CursorGetDescriptionResult
    group = "Database"
