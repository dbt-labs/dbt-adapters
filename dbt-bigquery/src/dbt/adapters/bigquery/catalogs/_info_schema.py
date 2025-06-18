from dbt.adapters.catalogs import CatalogIntegration
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.bigquery import constants
from dbt.adapters.bigquery.catalogs._relation import BigQueryCatalogRelation


class BigQueryInfoSchemaCatalogIntegration(CatalogIntegration):
    catalog_type = constants.DEFAULT_INFO_SCHEMA_CATALOG.catalog_type
    allows_writes = True

    def build_relation(self, model: RelationConfig) -> BigQueryCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """

        return BigQueryCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.catalog_name,
            table_format=self.table_format,
            file_format=self.file_format,
            external_volume=None,
            storage_uri=None,
        )
