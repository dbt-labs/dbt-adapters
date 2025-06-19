from dataclasses import dataclass
from typing import Optional, Dict, Any

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
)
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake import constants, parse_model


@dataclass
class IcebergRestCatalogRelation:
    base_location: Optional[str]
    catalog_type: str = constants.DEFAULT_ICEBERG_REST_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_ICEBERG_REST_CATALOG.name
    table_format: Optional[str] = constants.ICEBERG_TABLE_FORMAT
    external_volume: Optional[str] = None
    file_format: Optional[str] = None
    cluster_by: Optional[str] = None
    automatic_clustering: Optional[bool] = False
    is_transient: Optional[bool] = False


class IcebergRestCatalogIntegration(CatalogIntegration):
    catalog_name = constants.DEFAULT_ICEBERG_REST_CATALOG.name
    catalog_type = constants.DEFAULT_ICEBERG_REST_CATALOG.catalog_type
    table_format = constants.ICEBERG_TABLE_FORMAT
    file_format = None  # Snowflake chooses based on stage-format
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        # we overwrite this because the base provides too much config
        self.name: str = config.name
        self.catalog_name: Optional[str] = config.catalog_name
        self.external_volume: Optional[str] = config.external_volume
        self.adapter_properties: Dict[str, Any] = config.adapter_properties or {}

    def build_relation(self, model: RelationConfig) -> IcebergRestCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        return IcebergRestCatalogRelation(
            base_location=parse_model.base_location(model),
            catalog_name=self.catalog_name,
            external_volume=parse_model.external_volume(model) or self.external_volume,
            cluster_by=parse_model.cluster_by(model),
            automatic_clustering=parse_model.automatic_clustering(model),
        ) 