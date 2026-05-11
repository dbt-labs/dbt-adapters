from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict


class V2TableFormat(str, Enum):
    DEFAULT = "default"
    ICEBERG = "iceberg"


@dataclass
class CatalogV2:
    """User-facing v2 catalog config parsed from catalogs.yml."""

    name: str
    catalog_type: str
    table_format: V2TableFormat
    config: Dict[str, Dict[str, Any]]  # platform → fields, free dict
