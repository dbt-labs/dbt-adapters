from typing import Union

from dbt.adapters.snowflake.catalogs._built_in import (
    BuiltInCatalogIntegration,
    BuiltInCatalogRelation,
)
from dbt.adapters.snowflake.catalogs._standard import (
    StandardCatalogIntegration,
    StandardCatalogRelation,
)


SnowflakeCatalogRelation = Union[
    BuiltInCatalogRelation,
    StandardCatalogRelation,
]


SnowflakeCatalogIntegration = Union[
    BuiltInCatalogIntegration,
    StandardCatalogIntegration,
]
