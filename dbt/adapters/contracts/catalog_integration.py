from dataclasses import dataclass

from dbt_common.dataclass_schema import StrEnum


class TableFormat(StrEnum):
    iceberg = "iceberg"


class CatalogIntegrationType(StrEnum):
    glue = "glue"


@dataclass
class CatalogIntegration:
    name: str
    external_volume: str
    table_format: TableFormat
    type: CatalogIntegrationType