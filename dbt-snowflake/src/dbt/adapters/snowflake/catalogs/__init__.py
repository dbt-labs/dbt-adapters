from types import SimpleNamespace
from typing import Union

from dbt.adapters.snowflake.catalogs._iceberg_managed import (
    IcebergManagedCatalogIntegration,
    IcebergManagedCatalogRelation,
)


SnowflakeCatalogRelation = Union[IcebergManagedCatalogRelation]
SnowflakeCatalogIntegration = Union[IcebergManagedCatalogIntegration]


CATALOG_INTEGRATIONS = {
    "iceberg_managed": IcebergManagedCatalogIntegration,
}


DEFAULT_ICEBERG_CATALOG_INTEGRATION = SimpleNamespace(
    **{
        "name": "default_iceberg_catalog",
        "catalog_name": "snowflake",
        "catalog_type": "iceberg_managed",
        "table_format": "iceberg",
        "external_volume": None,  # assume you are using the default volume or specify in the model
        "adapter_properties": {},
    }
)
