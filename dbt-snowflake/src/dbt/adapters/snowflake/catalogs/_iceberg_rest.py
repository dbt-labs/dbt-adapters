from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
)
from dbt.adapters.catalogs import InvalidCatalogIntegrationConfigError
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake import constants, parse_model

from dbt.adapters.exceptions.compilation import InvalidRelationConfigError


@dataclass
class IcebergRestCatalogRelation:
    catalog_type: str = constants.DEFAULT_ICEBERG_REST_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_ICEBERG_REST_CATALOG.name
    table_format: Optional[str] = constants.ICEBERG_TABLE_FORMAT
    catalog_linked_database: Optional[str] = None
    external_volume: Optional[str] = None
    rest_endpoint: Optional[str] = None
    file_format: Optional[str] = None
    automatic_clustering: Optional[bool] = False
    is_transient: Optional[bool] = False


class IcebergRestCatalogIntegration(CatalogIntegration):
    catalog_type = constants.DEFAULT_ICEBERG_REST_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_ICEBERG_REST_CATALOG.name
    table_format = constants.ICEBERG_TABLE_FORMAT
    file_format = None  # Snowflake chooses based on stage-format
    allows_writes = True
    rest_endpoint: Optional[str] = None

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        # we overwrite this because the base provides too much config
        self.name: str = config.name
        self.external_volume: Optional[str] = config.external_volume
        self.rest_endpoint: Optional[str] = None
        self.catalog_linked_database: Optional[str] = None
        if adapter_properties := config.adapter_properties:
            self.catalog_linked_database = adapter_properties.get("catalog_linked_database")
            self.rest_endpoint = adapter_properties.get("rest_endpoint")

        if not self.catalog_linked_database:
            raise InvalidCatalogIntegrationConfigError(
                config.name,
                "adapter_properties.catalog_linked_database is currently required for iceberg rest catalog integrations with catalog linked databases",
            )

    def build_relation(self, model: RelationConfig) -> IcebergRestCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        cluster_by = parse_model.cluster_by(model)
        if cluster_by:
            raise InvalidRelationConfigError(
                model,
                model.config,
                "cluster_by is not supported for iceberg rest catalog integrations with catalog linked databases",
            )

        return IcebergRestCatalogRelation(
            catalog_name=self.catalog_name,
            external_volume=parse_model.external_volume(model) or self.external_volume,
            rest_endpoint=parse_model.rest_endpoint(model) or self.rest_endpoint,
            automatic_clustering=parse_model.automatic_clustering(model),
            catalog_linked_database=self.catalog_linked_database,
        )
