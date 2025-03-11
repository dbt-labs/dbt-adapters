from dbt.adapters.snowflake.catalogs._iceberg_rest import (
    IcebergRESTCatalogIntegration,
    IcebergRESTCatalogIntegrationConfig,
)


# these are the valid values for `catalog_type`
CATALOG_INTEGRATIONS = {
    "iceberg_rest": IcebergRESTCatalogIntegration,
    "aws_glue": IcebergRESTCatalogIntegration,
}
