from types import SimpleNamespace
from typing import Union

from dbt.adapters.snowflake.catalogs._iceberg_managed import (
    IcebergManagedCatalogIntegration,
    IcebergManagedCatalogRelation,
)
from dbt.adapters.snowflake.catalogs._iceberg_rest import (
    IcebergAWSGlueCatalogIntegration,
    IcebergRESTCatalogIntegration,
    IcebergRESTCatalogRelation,
)


SnowflakeCatalogRelation = Union[
    IcebergManagedCatalogRelation,
    IcebergRESTCatalogRelation,
]
SnowflakeCatalogIntegration = Union[
    IcebergAWSGlueCatalogIntegration,
    IcebergManagedCatalogIntegration,
    IcebergRESTCatalogIntegration,
]


CATALOG_INTEGRATIONS = [
    IcebergAWSGlueCatalogIntegration,
    IcebergManagedCatalogIntegration,
    IcebergRESTCatalogIntegration,
]


DEFAULT_ICEBERG_CATALOG_INTEGRATION = SimpleNamespace(
    **{
        "name": "snowflake",
        "catalog_type": "iceberg_managed",
        # assume you are using the default volume or specify in the model
        "external_volume": None,
        "adapter_properties": {},
    }
)
