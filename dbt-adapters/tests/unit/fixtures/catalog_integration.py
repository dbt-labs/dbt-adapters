from dataclasses import dataclass

from dbt.adapters.catalogs import CatalogIntegration


@dataclass
class CatalogIntegrationStub(CatalogIntegration):
    name: str
    catalog_type: str = "managed"
    table_format: str = "iceberg"
