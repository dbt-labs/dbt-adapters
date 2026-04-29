from dbt.adapters.bigquery.catalogs._biglake_metastore import BigLakeCatalogIntegration
from dbt.adapters.bigquery.catalogs._info_schema import BigQueryInfoSchemaCatalogIntegration
from dbt.adapters.bigquery.catalogs._relation import BigQueryCatalogRelation

# Import _v2 for its side effect of registering platform configs
from dbt.adapters.bigquery.catalogs import _v2  # noqa: F401
