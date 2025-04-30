from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from agate import Table


def _column_filter(val: Any) -> Any:
    return (
        float(val)
        if isinstance(val, Decimal)
        else (
            str(val)
            if isinstance(val, datetime)
            else str(val) if isinstance(val, date) else str(val)
        )
    )


def serialize_agate_table(table: "Table") -> Dict[str, Any]:
    rows = []
    for row in table.rows:
        row = list(map(_column_filter, row))
        rows.append(row)

    return {
        "column_names": table.column_names,
        "column_types": [t.__class__.__name__ for t in table.column_types],
        "rows": rows,
    }
