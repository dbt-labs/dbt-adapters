from dbt.adapters.snowflake.catalogs._aws_glue import AWSGlueCatalog
from dbt.adapters.snowflake.catalogs._iceberg_rest import IcebergRESTCatalog


# these are the valid values for `catalog_type`
CATALOG_INTEGRATIONS = {
    "iceberg_rest": IcebergRESTCatalog,
    "glue": AWSGlueCatalog,
}
