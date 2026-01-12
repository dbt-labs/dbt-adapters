import dataclasses
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, TYPE_CHECKING, List, Union, Optional

from mashumaro.types import SerializationStrategy
from dbt_common.record import get_record_row_limit_from_env

RECORDER_ROW_LIMIT: Optional[int] = get_record_row_limit_from_env()

if TYPE_CHECKING:
    from agate import Table
    from dbt.adapters.base.relation import BaseRelation
    from dbt.adapters.base.column import Column as BaseColumn


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

    if RECORDER_ROW_LIMIT and len(table.rows) > RECORDER_ROW_LIMIT:
        rows = [
            [
                f"Recording Error: Agate table contains {len(table.rows)} rows, maximum is {RECORDER_ROW_LIMIT} rows."
            ]
        ]
    else:
        for row in table.rows:
            row = list(map(_column_filter, row))
            rows.append(row)

    return {
        "column_names": table.column_names,
        "column_types": [t.__class__.__name__ for t in table.column_types],
        "rows": rows,
    }


def deserialize_agate_table(data: Dict[str, Any]) -> "Table":
    """Deserialize an agate Table from a dictionary.

    This function reconstructs an agate Table from the serialized format
    produced by serialize_agate_table().

    Note: DateTime values are stored as strings during serialization via _column_filter().
    During deserialization, we use Text type for DateTime columns since the original
    datetime objects were converted to strings. This maintains data integrity for replay
    purposes where the exact string representation is what matters.
    """
    import agate

    column_names = data.get("column_names", [])
    column_type_names = data.get("column_types", [])
    rows = data.get("rows", [])

    # Map type names to agate column types
    # Note: DateTime and Date are stored as strings (via str() in _column_filter),
    # so we use Text type to avoid parsing issues with timezone-aware strings
    type_map = {
        "Text": agate.Text(),
        "Number": agate.Number(),
        "Boolean": agate.Boolean(),
        "Date": agate.Text(),  # Stored as ISO string
        "DateTime": agate.Text(),  # Stored as ISO string with timezone
        "TimeDelta": agate.Text(),  # Stored as string
        # Integer is often stored as Number in agate
        "Integer": agate.Number(),
    }

    column_types = []
    for type_name in column_type_names:
        column_types.append(type_map.get(type_name, agate.Text()))

    return agate.Table(rows, column_names, column_types)


def serialize_bindings(bindings: Any) -> Union[None, List[Any], str]:
    if bindings is None:
        return None
    elif isinstance(bindings, list):
        return list(map(_column_filter, bindings))
    else:
        return "bindings"


def serialize_base_relation(relation: "BaseRelation") -> Dict[str, Any]:
    """Serialize a BaseRelation object for recording."""
    return relation.to_dict(omit_none=True)


def serialize_base_relation_list(relations: List["BaseRelation"]) -> List[Dict[str, Any]]:
    """Serialize a list of BaseRelation objects for recording."""
    if RECORDER_ROW_LIMIT and len(relations) > RECORDER_ROW_LIMIT:
        return [
            {
                "error": f"Recording Error: List of BaseRelation objects contains {len(relations)} objects, maximum is {RECORDER_ROW_LIMIT} objects."
            }
        ]
    else:
        return [serialize_base_relation(relation) for relation in relations]


def deserialize_base_relation(relation_dict: Dict[str, Any]) -> "BaseRelation":
    """Deserialize a BaseRelation object from a dictionary.

    This function attempts to create the appropriate adapter-specific relation
    based on the fields present in the dictionary. For example, if 'table_format'
    is present, it creates a SnowflakeRelation.
    """
    # Check for adapter-specific fields to determine which relation class to use
    if "table_format" in relation_dict:
        # This is a Snowflake relation
        try:
            from dbt.adapters.snowflake.relation import SnowflakeRelation

            return SnowflakeRelation.from_dict(relation_dict)
        except ImportError:
            pass  # Fall through to BaseRelation if snowflake adapter not available

    # Default to BaseRelation
    from dbt.adapters.base.relation import BaseRelation

    return BaseRelation.from_dict(relation_dict)


def deserialize_base_relation_list(relations_data: List[Dict[str, Any]]) -> List["BaseRelation"]:
    """Deserialize a list of BaseRelation objects from dictionaries."""
    return [deserialize_base_relation(relation_dict) for relation_dict in relations_data]


def serialize_base_column_list(columns: List["BaseColumn"]) -> List[Dict[str, Any]]:
    if RECORDER_ROW_LIMIT and len(columns) > RECORDER_ROW_LIMIT:
        return [
            {
                "error": f"Recording Error: List of BaseColumn objects contains {len(columns)} objects, maximum is {RECORDER_ROW_LIMIT} objects."
            }
        ]
    else:
        return [serialize_base_column(column) for column in columns]


def serialize_base_column(column: "BaseColumn") -> Dict[str, Any]:
    column_dict = dataclasses.asdict(column)
    return column_dict


def deserialize_base_column_list(columns_data: List[Dict[str, Any]]) -> List["BaseColumn"]:
    return [deserialize_base_column(column_dict) for column_dict in columns_data]


def deserialize_base_column(column_dict: Dict[str, Any]) -> "BaseColumn":
    from dbt.adapters.base.column import Column as BaseColumn

    # Only include fields that are present in the base column class
    params_dict = {
        field.name: column_dict[field.name]
        for field in dataclasses.fields(BaseColumn)
        if field.name in column_dict
    }

    return BaseColumn(**params_dict)


# Custom serialization strategies for BaseRelation
# These ensure proper deserialization of adapter-specific relations during replay
class _BaseRelationSerializationStrategy(SerializationStrategy):
    """Custom serialization strategy for BaseRelation that preserves adapter-specific types."""

    def serialize(self, value: Any) -> Dict[str, Any]:
        return value.to_dict(omit_none=True)

    def deserialize(self, value: Dict[str, Any]) -> Any:
        return deserialize_base_relation(value)


class _OptionalBaseRelationSerializationStrategy(SerializationStrategy):
    """Custom serialization strategy for Optional[BaseRelation]."""

    def serialize(self, value: Any) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        return value.to_dict(omit_none=True)

    def deserialize(self, value: Optional[Dict[str, Any]]) -> Any:
        if value is None:
            return None
        return deserialize_base_relation(value)


def _register_relation_serialization_strategies():
    """Register serialization strategies for BaseRelation types.

    This is needed for auto_record_function to properly serialize/deserialize
    relations during record/replay. Without this, relations would be deserialized
    as BaseRelation instead of their adapter-specific types (e.g., SnowflakeRelation).
    """
    from dbt_common.record import Recorder
    from dbt.adapters.base.relation import BaseRelation

    Recorder.register_serialization_strategy(BaseRelation, _BaseRelationSerializationStrategy())
    Recorder.register_serialization_strategy(
        Optional[BaseRelation], _OptionalBaseRelationSerializationStrategy()
    )


# Register strategies when this module is imported
_register_relation_serialization_strategies()
