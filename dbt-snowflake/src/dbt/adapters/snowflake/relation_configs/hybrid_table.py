from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional, Tuple, TYPE_CHECKING

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase

if TYPE_CHECKING:  # pragma: no cover
    import agate


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.strip().split())


def _extract_data_type(definition: str) -> str:
    upper_definition = definition.upper()
    stop_keywords = [
        " NOT NULL",
        " NULL",
        " DEFAULT ",
        " AUTOINCREMENT",
        " IDENTITY",
        " COMMENT ",
    ]
    end_index = len(definition)
    for keyword in stop_keywords:
        position = upper_definition.find(keyword)
        if position != -1 and position < end_index:
            end_index = position
    return _normalize_whitespace(definition[:end_index])


def _extract_default(definition: str) -> Optional[str]:
    upper_definition = definition.upper()
    default_position = upper_definition.find(" DEFAULT ")
    if default_position == -1:
        return None
    start = default_position + len(" DEFAULT ")
    default_clause = definition[start:]
    stop_keywords = [" AUTOINCREMENT", " IDENTITY", " COMMENT "]
    end_index = len(default_clause)
    upper_clause = default_clause.upper()
    for keyword in stop_keywords:
        pos = upper_clause.find(keyword)
        if pos != -1 and pos < end_index:
            end_index = pos
    return _normalize_whitespace(default_clause[:end_index]) or None


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeHybridTableColumn:
    name: str
    definition: str
    data_type: str
    nullable: bool
    default: Optional[str] = None
    autoincrement: bool = False

    @classmethod
    def from_column_definition(cls, column_name: str, definition: str) -> "SnowflakeHybridTableColumn":
        normalized = _normalize_whitespace(definition)
        data_type = _extract_data_type(normalized)
        nullable = "NOT NULL" not in normalized.upper()
        default_value = _extract_default(normalized)
        autoincrement = "AUTOINCREMENT" in normalized.upper() or "IDENTITY" in normalized.upper()
        return cls(
            name=column_name,
            definition=normalized,
            data_type=data_type,
            nullable=nullable,
            default=default_value,
            autoincrement=autoincrement,
        )

    @classmethod
    def from_describe_row(cls, row: "agate.Row") -> "SnowflakeHybridTableColumn":
        column_name = row.get("name")
        column_type = row.get("type")
        nullable_flag = row.get("null?")
        default_value = row.get("default") or None
        autoincrement = False
        if isinstance(default_value, str):
            autoincrement = "AUTOINCREMENT" in default_value.upper() or "IDENTITY" in default_value.upper()
        definition = column_type or ""
        if nullable_flag == "N":
            definition = f"{definition} NOT NULL"
        if default_value:
            definition = f"{definition} DEFAULT {default_value}"
        return cls(
            name=column_name,
            definition=_normalize_whitespace(definition),
            data_type=_normalize_whitespace(column_type or ""),
            nullable=nullable_flag != "N",
            default=_normalize_whitespace(default_value) if default_value else None,
            autoincrement=autoincrement,
        )


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeHybridTableConfig(SnowflakeRelationConfigBase):
    name: Optional[str] = None
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    columns: Tuple[SnowflakeHybridTableColumn, ...] = field(default_factory=tuple)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SnowflakeHybridTableConfig":
        columns = tuple(config_dict.get("columns", ()))
        config_dict = {**config_dict, "columns": columns}
        return super().from_dict(config_dict)  # type: ignore

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        column_definitions = relation_config.config.extra.get("column_definitions", {})  # type: ignore
        column_order: Optional[Iterable[str]] = relation_config.config.extra.get("column_order")  # type: ignore
        if not column_order:
            column_order = sorted(column_definitions.keys())
        columns = tuple(
            SnowflakeHybridTableColumn.from_column_definition(column_name, column_definitions[column_name])
            for column_name in column_order
            if column_name in column_definitions
        )
        return {
            "name": relation_config.identifier,
            "schema_name": relation_config.schema,
            "database_name": relation_config.database,
            "columns": columns,
        }

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        columns_table: "agate.Table" = relation_results["columns"]  # type: ignore
        columns = tuple(SnowflakeHybridTableColumn.from_describe_row(row) for row in columns_table)
        return {
            "name": None,
            "schema_name": None,
            "database_name": None,
            "columns": columns,
        }


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeHybridTableColumnTypeChange(RelationConfigChange):
    column_name: str
    existing_type: str
    new_type: str

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass
class SnowflakeHybridTableConfigChangeset:
    add_columns: Tuple[SnowflakeHybridTableColumn, ...] = ()
    drop_columns: Tuple[SnowflakeHybridTableColumn, ...] = ()
    type_changes: Tuple[SnowflakeHybridTableColumnTypeChange, ...] = ()

    @property
    def has_changes(self) -> bool:
        return any([self.add_columns, self.drop_columns, self.type_changes])

    @property
    def requires_full_refresh(self) -> bool:
        return any(change.requires_full_refresh for change in self.type_changes)


# Snowflake type aliases: map user-facing names to canonical DESCRIBE output
_SNOWFLAKE_TYPE_ALIASES: Dict[str, str] = {
    "INT": "NUMBER(38,0)",
    "INTEGER": "NUMBER(38,0)",
    "BIGINT": "NUMBER(38,0)",
    "SMALLINT": "NUMBER(38,0)",
    "TINYINT": "NUMBER(38,0)",
    "BYTEINT": "NUMBER(38,0)",
    "FLOAT": "FLOAT",
    "FLOAT4": "FLOAT",
    "FLOAT8": "FLOAT",
    "DOUBLE": "FLOAT",
    "DOUBLE PRECISION": "FLOAT",
    "REAL": "FLOAT",
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP_NTZ(9)",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ(9)",
    "TIMESTAMP_LTZ": "TIMESTAMP_LTZ(9)",
    "TIMESTAMP_TZ": "TIMESTAMP_TZ(9)",
    "TIME": "TIME(9)",
    "STRING": "VARCHAR(16777216)",
    "TEXT": "VARCHAR(16777216)",
    "VARCHAR": "VARCHAR(16777216)",
    "BINARY": "BINARY(8388608)",
    "VARBINARY": "BINARY(8388608)",
    "VARIANT": "VARIANT",
    "OBJECT": "OBJECT",
    "ARRAY": "ARRAY",
}


def _canonicalize_snowflake_type(data_type: str) -> str:
    """Normalize a Snowflake data type to its canonical DESCRIBE TABLE form."""
    upper = _normalize_whitespace(data_type).upper()
    # Check for exact alias match first (e.g. INT -> NUMBER(38,0))
    if upper in _SNOWFLAKE_TYPE_ALIASES:
        return _SNOWFLAKE_TYPE_ALIASES[upper]
    # Handle DECIMAL/NUMERIC/NUMBER without precision -> NUMBER(38,0)
    if upper in ("DECIMAL", "NUMERIC", "NUMBER"):
        return "NUMBER(38,0)"
    # Normalize DECIMAL(p,s) and NUMERIC(p,s) -> NUMBER(p,s)
    m = re.match(r"(DECIMAL|NUMERIC)\((\d+),\s*(\d+)\)", upper)
    if m:
        return f"NUMBER({m.group(2)},{m.group(3)})"
    # Already canonical form - return as-is
    return upper


def _as_dict(columns: Tuple[SnowflakeHybridTableColumn, ...]) -> Dict[str, SnowflakeHybridTableColumn]:
    return {column.name.lower(): column for column in columns}


def build_hybrid_table_changeset(
    existing: SnowflakeHybridTableConfig,
    new: SnowflakeHybridTableConfig,
) -> SnowflakeHybridTableConfigChangeset:
    existing_columns = _as_dict(existing.columns)
    new_columns = _as_dict(new.columns)

    add_columns = tuple(
        column for name, column in new_columns.items() if name not in existing_columns
    )
    drop_columns = tuple(
        column for name, column in existing_columns.items() if name not in new_columns
    )

    type_changes: Tuple[SnowflakeHybridTableColumnTypeChange, ...] = ()
    for name, column in new_columns.items():
        if name in existing_columns:
            existing_column = existing_columns[name]
            if _canonicalize_snowflake_type(existing_column.data_type) != _canonicalize_snowflake_type(
                column.data_type
            ):
                change = SnowflakeHybridTableColumnTypeChange(
                    action=RelationConfigChangeAction.alter,  # type: ignore
                    context=column,
                    column_name=column.name,
                    existing_type=existing_column.data_type,
                    new_type=column.data_type,
                )
                type_changes = (*type_changes, change)

    return SnowflakeHybridTableConfigChangeset(
        add_columns=add_columns,
        drop_columns=drop_columns,
        type_changes=type_changes,
    )
