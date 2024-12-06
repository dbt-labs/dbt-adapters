from dataclasses import dataclass

from dbt_common.dataclass_schema import StrEnum, dbtClassMixin


class TableFormat(StrEnum):
    iceberg = "iceberg"


class CatalogIntegrationType(StrEnum):
    glue = "glue"


@dataclass
class CatalogIntegration(dbtClassMixin):
    name: str
    external_volume: str
    table_format: TableFormat
    catalog_type: CatalogIntegrationType