"""Column types are kept as ``describe`` reports them; the base class would rebuild
``VARCHAR`` as ``character varying(256)``."""

from dataclasses import dataclass
from typing import ClassVar, Dict

from dbt.adapters.base.column import Column

_STRING_TYPES = frozenset({"varchar", "text", "string", "char", "bpchar"})


@dataclass
class LakeComputeColumn(Column):
    # no STRING -> TEXT translation: contracts compare the literal type names
    TYPE_LABELS: ClassVar[Dict[str, str]] = {}

    @property
    def data_type(self) -> str:
        return self.dtype

    def is_string(self) -> bool:
        return self.dtype.lower().split("(")[0].strip() in _STRING_TYPES

    def is_numeric(self) -> bool:
        return self.dtype.lower().split("(")[0].strip() in ("decimal", "numeric")

    def is_float(self) -> bool:
        return self.dtype.lower() in ("real", "float", "float4", "float8", "double")

    def is_integer(self) -> bool:
        return self.dtype.lower() in (
            "tinyint",
            "smallint",
            "integer",
            "int",
            "bigint",
            "hugeint",
            "utinyint",
            "usmallint",
            "uinteger",
            "ubigint",
            "uhugeint",
            "int1",
            "int2",
            "int4",
            "int8",
        )

    def __repr__(self) -> str:
        return "<LakeComputeColumn {} ({})>".format(self.name, self.data_type)
