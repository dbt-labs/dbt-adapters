"""Arrow type -> DuckDB type-name mapping, and result normalisation.

ADBC's ``cursor.description`` is PEP-249 shaped but ``description[i][1]`` holds a **pyarrow
``DataType``, not an integer type code**. dbt calls ``data_type_code_to_name`` on that value to
label columns in ``dbt show`` and to check model contracts, so without a mapping every column
reports as ``int64``/``string`` rather than a database type name.

Kept in its own module so it can be unit-tested without an adapter or a connection.
"""

from datetime import date
from decimal import Decimal
from typing import Any, Dict

#: exact arrow type string -> DuckDB name. Prefix cases (decimal, timestamp) are handled below.
_ARROW_TO_DUCKDB: Dict[str, str] = {
    "null": "NULL",
    "bool": "BOOLEAN",
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "uint8": "UTINYINT",
    "uint16": "USMALLINT",
    "uint32": "UINTEGER",
    "uint64": "UBIGINT",
    "halffloat": "FLOAT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "string": "VARCHAR",
    "large_string": "VARCHAR",
    "string_view": "VARCHAR",
    "binary": "BLOB",
    "large_binary": "BLOB",
    "binary_view": "BLOB",
    "date32[day]": "DATE",
    "date64[ms]": "DATE",
    "month_day_nano_interval": "INTERVAL",
}


def data_type_code_to_name(type_code: Any) -> str:
    """Translate one ``cursor.description`` type entry into a DuckDB type name.

    Accepts a pyarrow ``DataType`` (what ADBC actually supplies), or anything whose ``str()``
    looks like an arrow type string. Unknown types come back uppercased rather than raising:
    a wrong label in ``dbt show`` is a cosmetic problem, an exception there is not.
    """
    name = str(type_code).strip().lower()

    if name in _ARROW_TO_DUCKDB:
        return _ARROW_TO_DUCKDB[name]

    # decimal128(18, 3) / decimal256(38, 9) -> DECIMAL(18,3)
    if name.startswith("decimal"):
        inner = name[name.find("(") + 1 : name.rfind(")")] if "(" in name else ""
        parts = [p.strip() for p in inner.split(",") if p.strip()]
        if len(parts) == 2:
            return f"DECIMAL({parts[0]},{parts[1]})"
        return "DECIMAL"

    # timestamp[us] / timestamp[ns, tz=UTC] -> TIMESTAMP / TIMESTAMP WITH TIME ZONE
    if name.startswith("timestamp"):
        return "TIMESTAMP WITH TIME ZONE" if "tz=" in name else "TIMESTAMP"

    if name.startswith("time32") or name.startswith("time64"):
        return "TIME"

    if name.startswith("duration"):
        return "INTERVAL"

    if name.startswith("dictionary"):
        return "VARCHAR"

    if (
        name.startswith("list")
        or name.startswith("large_list")
        or name.startswith("fixed_size_list")
    ):
        return "LIST"

    if name.startswith("struct"):
        return "STRUCT"

    if name.startswith("map"):
        return "MAP"

    return name.upper()


def normalize_value(value: Any) -> Any:
    """Make one cell safe for agate and for ``run_results.json``.

    ``table_from_data_flat`` JSON-encodes dict/list/tuple values, and dbt's
    ``ForgivingJSONEncoder`` can reject raw bytes. Rather than let that surface as an opaque
    serialisation error deep in a run, convert here:

    * ``bytes``/``bytearray``/``memoryview`` -> hex string
    * ``Decimal``, ``str``, ``bool``, ``int``, ``float``, ``None`` -> unchanged
    * ``date``/``datetime`` -> unchanged, so agate infers Date/DateTime: ``source freshness``
      needs a real datetime, and macros doing date maths on ``run_query`` results expect one
    * dict/list/tuple -> unchanged (agate_helper handles these)
    * anything else (``time``, ``timedelta``, ...) -> ``str()``; agate has no type for them
    """
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if value is None or isinstance(value, (str, bool, int, float, Decimal, date)):
        return value
    if isinstance(value, (dict, list, tuple)):
        return value
    return str(value)
