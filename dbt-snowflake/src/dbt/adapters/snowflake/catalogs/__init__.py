from dbt.adapters.snowflake.catalogs._iceberg_managed import (
    IcebergManagedCatalogIntegration,
    IcebergManagedCatalogRelation,
)
from dbt.adapters.snowflake.catalogs._iceberg_rest import (
    IcebergAWSGlueCatalogIntegration,
    IcebergRESTCatalogIntegration,
    IcebergRESTCatalogRelation,
)
from dbt.adapters.snowflake.catalogs._native import (
    NativeCatalogIntegration,
    NativeCatalogRelation,
)
