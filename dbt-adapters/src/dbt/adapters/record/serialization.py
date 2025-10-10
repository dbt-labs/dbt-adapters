import dataclasses
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, TYPE_CHECKING, List, Union

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


def serialize_base_relation(relation: "BaseRelation") -> Dict[str, Any]:
    """Serialize a BaseRelation object for recording."""
    return relation.to_dict(omit_none=True)


def serialize_base_relation_list(relations: List["BaseRelation"]) -> List[Dict[str, Any]]:
    """Serialize a list of BaseRelation objects for recording."""
    return [serialize_base_relation(relation) for relation in relations]


def deserialize_base_relation(relation_dict: Dict[str, Any]) -> "BaseRelation":
    """Deserialize a BaseRelation object from a dictionary."""
    from dbt.adapters.base.relation import BaseRelation

    return BaseRelation.from_dict(relation_dict)


def deserialize_base_relation_list(relations_data: List[Dict[str, Any]]) -> List["BaseRelation"]:
    """Deserialize a list of BaseRelation objects from dictionaries."""
    return [deserialize_base_relation(relation_dict) for relation_dict in relations_data]


def serialize_base_column_list(columns: List["BaseColumn"]) -> List[Dict[str, Any]]:
    return [serialize_base_column(column) for column in columns]


def serialize_base_column(column: "BaseColumn") -> Dict[str, Any]:
    column_dict = dataclasses.asdict(column)
    return column_dict


def deserialize_base_column_list(columns_data: List[Dict[str, Any]]) -> List["BaseColumn"]:
    return [deserialize_base_column(column_dict) for column_dict in columns_data]


def deserialize_base_column(column_dict: Dict[str, Any]) -> "BaseColumn":
    # Only include fields that are present in the base column class
    params_dict = {
        field.name: column_dict[field.name]
        for field in dataclasses.fields(BaseColumn)
        if field.name in column_dict
    }

    return BaseColumn(**params_dict)
