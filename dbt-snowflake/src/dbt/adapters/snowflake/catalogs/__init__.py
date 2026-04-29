from dbt.adapters.snowflake.catalogs._built_in import (
    BuiltInCatalogIntegration,
    BuiltInCatalogRelation,
)
from dbt.adapters.snowflake.catalogs._info_schema import (
    InfoSchemaCatalogIntegration,
    InfoSchemaCatalogRelation,
)
from dbt.adapters.snowflake.catalogs._iceberg_rest import (
    IcebergRestCatalogIntegration,
    IcebergRestCatalogRelation,
)

# Import _v2 for its side effect of registering platform configs
from dbt.adapters.snowflake.catalogs import _v2  # noqa: F401
