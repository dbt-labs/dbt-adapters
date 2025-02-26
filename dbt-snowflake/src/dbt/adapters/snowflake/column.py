from dataclasses import dataclass

from dbt.adapters.base.column import Column
from dbt_common.exceptions import DbtRuntimeError


@dataclass
class SnowflakeColumn(Column):
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

    @classmethod
    def from_description(cls, name: str, raw_data_type: str) -> "SnowflakeColumn":
        # We want to pass through numeric parsing for composite types
        if raw_data_type.lower().startswith(("array", "object", "map", "vector")):
            column = cls(name, raw_data_type, None, None, None)
        else:
            column = super().from_description(name, raw_data_type)  # type:ignore
        return column

    def is_array(self) -> bool:
        return self.dtype.lower().startswith("array")

    def is_object(self) -> bool:
        return self.dtype.lower().startswith("object")

    def is_map(self) -> bool:
        return self.dtype.lower().startswith("map")
