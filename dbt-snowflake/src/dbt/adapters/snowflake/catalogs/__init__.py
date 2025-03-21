from dbt.adapters.snowflake.catalogs._iceberg_managed import (
    IcebergManagedCatalogIntegration,
    ICEBERG_MANAGED_CATALOG,
)


CATALOG_INTEGRATIONS = {
    "iceberg_managed": IcebergManagedCatalogIntegration,
}
