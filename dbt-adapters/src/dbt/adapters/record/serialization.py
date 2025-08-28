from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, TYPE_CHECKING, List, Union

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


def serialize_bindings(bindings: Any) -> Union[None, List[Any], str]:
    if bindings is None:
        return None
    elif isinstance(bindings, list):
        return list(map(_column_filter, bindings))
    else:
        return "bindings"
