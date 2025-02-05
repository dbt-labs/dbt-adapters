from dbt.adapters.catalogs import CatalogIntegration


class CatalogIntegrationStub(CatalogIntegration):
    name: str
    catalog_type: str = "managed"
    table_format: str = "iceberg"
    external_volume: str = "my_external_volume"
