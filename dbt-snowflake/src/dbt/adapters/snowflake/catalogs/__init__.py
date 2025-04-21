from typing import Union

from dbt.adapters.snowflake.catalogs._built_in import (
    BuiltInCatalogIntegration,
    BuiltInCatalogRelation,
)
from dbt.adapters.snowflake.catalogs._local import (
    LocalCatalogIntegration,
    LocalCatalogRelation,
)


SnowflakeCatalogRelation = Union[
    BuiltInCatalogRelation,
    LocalCatalogRelation,
]


SnowflakeCatalogIntegration = Union[
    BuiltInCatalogIntegration,
    LocalCatalogIntegration,
]
