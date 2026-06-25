from dbt.adapters.catalogs import CatalogIntegration
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.athena import constants
from dbt.adapters.athena.catalogs._relation import AthenaCatalogRelation


class GlueCatalogIntegration(CatalogIntegration):
    """Writable Iceberg catalog backed by the AWS Glue Data Catalog."""

    catalog_type = constants.GLUE_CATALOG_TYPE
    table_format = constants.ICEBERG_TABLE_FORMAT
    file_format = constants.PARQUET_FILE_FORMAT
    allows_writes = True

    def build_relation(self, model: RelationConfig) -> AthenaCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        return AthenaCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.catalog_name,
            table_format=self.table_format,
            file_format=self.file_format,
            external_volume=self.external_volume,
        )
