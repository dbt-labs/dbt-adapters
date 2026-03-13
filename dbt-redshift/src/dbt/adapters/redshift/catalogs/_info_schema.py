from dbt.adapters.catalogs import CatalogIntegration
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.redshift import constants
from dbt.adapters.redshift.catalogs._relation import RedshiftCatalogRelation


class RedshiftInfoSchemaCatalogIntegration(CatalogIntegration):
    """
    Default catalog integration for standard Redshift tables.

    This represents the traditional Redshift table storage where tables
    are stored in the Redshift cluster's internal storage and metadata
    is tracked in the information_schema.

    This is the default catalog for backward compatibility - all existing
    dbt-redshift models will continue to work without any changes.
    """

    catalog_type = constants.INFO_SCHEMA_CATALOG_TYPE
    table_format = constants.DEFAULT_TABLE_FORMAT
    file_format = constants.DEFAULT_FILE_FORMAT
    allows_writes = True

    def build_relation(self, model: RelationConfig) -> RedshiftCatalogRelation:
        """
        Build a catalog relation for standard Redshift tables.

        Args:
            model: `config.model` (not `model`) from the jinja context

        Returns:
            RedshiftCatalogRelation configured for standard Redshift tables
        """
        return RedshiftCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.catalog_name,
            table_format=self.table_format,
            file_format=self.file_format,
        )
