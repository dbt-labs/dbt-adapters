from types import SimpleNamespace
from typing import Union

from dbt.adapters.snowflake.catalogs._iceberg_managed import (
    IcebergManagedCatalogIntegration,
    IcebergManagedCatalogRelation,
)


SnowflakeCatalogRelation = Union[IcebergManagedCatalogRelation]
SnowflakeCatalogIntegration = Union[IcebergManagedCatalogIntegration]


CATALOG_INTEGRATIONS = [IcebergManagedCatalogIntegration]


DEFAULT_ICEBERG_CATALOG_INTEGRATION = SimpleNamespace(
    **{
        "name": "snowflake",
        "catalog_type": "iceberg_managed",
        # assume you are using the default volume or specify in the model
        "external_volume": None,
        "adapter_properties": {},
    }
)
