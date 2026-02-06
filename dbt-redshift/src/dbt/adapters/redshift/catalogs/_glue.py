from typing import Optional

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
    InvalidCatalogIntegrationConfigError,
)
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.redshift import constants, parse_model
from dbt.adapters.redshift.catalogs._relation import RedshiftCatalogRelation


class GlueCatalogIntegration(CatalogIntegration):
    """
    Catalog integration for AWS Glue Data Catalog with Iceberg tables.

    This enables dbt models to be persisted as Iceberg tables in AWS Glue
    Data Catalog, accessible through Redshift external schemas.

    Configuration in dbt_project.yml:
        catalogs:
          - name: my_glue_catalog
            catalog_type: glue
            external_volume: s3://my-bucket/iceberg-data
            adapter_properties:
              glue_database: my_glue_db
              external_schema: my_external_schema

    Model configuration:
        {{ config(
            materialized='table',
            catalog='my_glue_catalog',
            partition_by=['date_column']
        ) }}
    """

    catalog_type = constants.GLUE_CATALOG_TYPE
    table_format = constants.ICEBERG_TABLE_FORMAT
    file_format = constants.PARQUET_FILE_FORMAT
    allows_writes = True

    # Adapter-specific properties
    glue_database: Optional[str] = None
    external_schema: Optional[str] = None

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)

        # Parse adapter-specific properties
        if adapter_properties := getattr(config, "adapter_properties", None):
            self.glue_database = adapter_properties.get("glue_database")
            self.external_schema = adapter_properties.get("external_schema")

        # Validate required configuration
        if not self.external_schema:
            raise InvalidCatalogIntegrationConfigError(
                config.name,
                "adapter_properties.external_schema is required for Glue catalog integrations. "
                "This should be the name of the Redshift external schema that points to your "
                "Glue Data Catalog database.",
            )

    def build_relation(self, model: RelationConfig) -> RedshiftCatalogRelation:
        """
        Build a catalog relation for Glue/Iceberg tables.

        Args:
            model: `config.model` (not `model`) from the jinja context

        Returns:
            RedshiftCatalogRelation configured for Glue/Iceberg tables
        """
        return RedshiftCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.name,
            table_format=self.table_format,
            file_format=parse_model.file_format(model) or self.file_format,
            external_volume=self.external_volume,
            storage_uri=self._calculate_storage_uri(model),
            glue_database=self.glue_database,
            external_schema=parse_model.external_schema(model) or self.external_schema,
            partition_by=parse_model.partition_by(model),
        )

    def _calculate_storage_uri(self, model: RelationConfig) -> Optional[str]:
        """
        Calculate the S3 storage URI for the Iceberg table.

        Priority:
        1. Explicit storage_uri in model config
        2. Auto-generated from external_volume + schema + model name

        Args:
            model: RelationConfig from the jinja context

        Returns:
            The S3 URI for the table data, or None if external_volume is not set
        """
        if model.config is None:
            return None

        # Priority 1: Explicit storage_uri in model config
        if model_storage_uri := parse_model.storage_uri(model):
            return model_storage_uri

        # Priority 2: Auto-generate from external_volume
        if not self.external_volume:
            return None

        # Build path: {external_volume}/{schema}/{model_name}/
        # The trailing slash is important for Iceberg table locations
        schema = model.schema or "_default"
        name = model.name

        storage_uri = f"{self.external_volume.rstrip('/')}/{schema}/{name}/"
        return storage_uri
