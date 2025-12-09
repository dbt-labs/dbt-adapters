from dataclasses import dataclass
import re
from typing import Optional

from dbt.adapters.base.column import Column
from dbt_common.exceptions import DbtRuntimeError

COLLATE_PATTERN = re.compile(r"COLLATE\s+'([^']+)'(\s*rtrim)?")


@dataclass
class SnowflakeColumn(Column):
    collation: Optional[str] = None

    def is_integer(self) -> bool:
        # everything that smells like an int is actually a NUMBER(38, 0)
        return False

    def is_numeric(self) -> bool:
        return self.dtype.lower() in [
            "int",
            "integer",
            "bigint",
            "smallint",
            "tinyint",
            "byteint",
            "numeric",
            "decimal",
            "number",
        ]

    def is_float(self):
        return self.dtype.lower() in [
            "float",
            "float4",
            "float8",
            "double",
            "double precision",
            "real",
        ]

    def string_size(self) -> int:
        if not self.is_string():
            raise DbtRuntimeError("Called string_size() on non-string field!")

        if self.dtype == "text" or self.char_size is None:
            return 16777216
        else:
            return int(self.char_size)

    @property
    def data_type(self) -> str:
        """Override data_type property to include collation for string types."""
        base_type = super().data_type

        # Add collation specification if present and this is a string type
        if self.collation:
            return f"{base_type} COLLATE '{self.collation}'"

        return base_type

    @classmethod
    def from_description(cls, name: str, raw_data_type: str) -> "SnowflakeColumn":
        """
        Parse column information from raw data type string.
        Handles Snowflake-specific syntax including COLLATE clause.

        Examples:
            VARCHAR(16777216) COLLATE 'en-ci-rtrim'
            VARCHAR(100)
            NUMBER(38,0)
        """

        # We want to pass through numeric parsing for composite types
        if raw_data_type.lower().startswith(("array", "object", "map", "vector")):
            return cls(name, raw_data_type, None, None, None)

        collation = None
        # Check if there's a COLLATE clause and extract it
        if raw_data_type.lower().startswith(
            ("varchar", "character varying", "character", "varchar", "text")
        ):
            collate_match = COLLATE_PATTERN.search(raw_data_type, re.IGNORECASE)
            collation = collate_match.group(1) if collate_match else None

        # Parse the base type using parent class logic
        base_column = super().from_description(name, raw_data_type)

        # Create a SnowflakeColumn with the parsed information plus collation
        return cls(
            column=base_column.column,
            dtype=base_column.dtype,
            char_size=base_column.char_size,
            numeric_precision=base_column.numeric_precision,
            numeric_scale=base_column.numeric_scale,
            collation=collation,
        )

    def is_array(self) -> bool:
        return self.dtype.lower().startswith("array")

    def is_object(self) -> bool:
        return self.dtype.lower().startswith("object")

    def is_map(self) -> bool:
        return self.dtype.lower().startswith("map")
