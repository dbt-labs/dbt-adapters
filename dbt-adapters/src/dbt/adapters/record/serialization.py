from datetime import datetime, date
from decimal import Decimal
from typing import TYPE_CHECKING, Tuple, Dict, Any

from dbt.adapters.contracts.connection import AdapterResponse

import agate

from mashumaro.types import SerializationStrategy


def _column_filter(val: Any) -> Any:
    return float(val) if isinstance(val, Decimal) else str(val) if isinstance(val, datetime) else str(val) if isinstance(val, date) else str(val)


def _serialize_agate_table(table: agate.Table) -> Dict[str, Any]:
    rows = []
    for row in table.rows:
        row = list(map(_column_filter, row))
        rows.append(row)

    return {
        "column_names": table.column_names,
        "column_types": [t.__class__.__name__ for t in table.column_types],
        "rows": rows
    }


class AdapterExecuteSerializer(SerializationStrategy):
    def serialize(self, table: Tuple[AdapterResponse, agate.Table]):
        adapter_response, agate_table = table
        return {
            "adapter_response": adapter_response.to_dict(),
            "table": _serialize_agate_table(agate_table)
        }

    def deserialize(self, data):
        # TODO:
        adapter_response_dct, agate_table_dct = data
        return None

class PartitionsMetadataSerializer(SerializationStrategy):
    def serialize(self, tables: Tuple[agate.Table]):
        return list(map(_serialize_agate_table, tables))

    def deserialize(self, data):
        # TODO:
        return None

class AgateTableSerializer(SerializationStrategy):
    def serialize(self, table: agate.Table):
        return _serialize_agate_table(table)

    def deserialize(self, data):
        # TODO:
        return None