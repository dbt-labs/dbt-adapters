from typing import Union

from dbt.adapters.snowflake.catalogs._iceberg_managed import (
    IcebergManagedCatalogIntegration,
    IcebergManagedCatalogRelation,
)


SnowflakeCatalogRelation = Union[IcebergManagedCatalogRelation]
SnowflakeCatalogIntegration = Union[IcebergManagedCatalogIntegration]


CATALOG_INTEGRATIONS = [IcebergManagedCatalogIntegration]
