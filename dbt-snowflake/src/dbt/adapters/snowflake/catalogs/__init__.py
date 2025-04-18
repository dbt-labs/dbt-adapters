from typing import Union

from dbt.adapters.snowflake.catalogs._iceberg_managed import (
    IcebergManagedCatalogIntegration,
    IcebergManagedCatalogRelation,
)
from dbt.adapters.snowflake.catalogs._native import (
    NativeCatalogIntegration,
    NativeCatalogRelation,
)


SnowflakeCatalogRelation = Union[
    IcebergManagedCatalogRelation,
    NativeCatalogRelation,
]


SnowflakeCatalogIntegration = Union[
    IcebergManagedCatalogIntegration,
    NativeCatalogIntegration,
]
