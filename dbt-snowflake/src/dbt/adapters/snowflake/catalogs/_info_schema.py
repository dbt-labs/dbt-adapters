from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
)
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake import constants, parse_model
from dbt.adapters.snowflake.catalogs._common import resolve_change_tracking
from dbt.adapters.snowflake.constants import SnowflakeIcebergTableRelationParameters


@dataclass
class InfoSchemaCatalogRelation:
    catalog_type: str = constants.DEFAULT_INFO_SCHEMA_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_INFO_SCHEMA_CATALOG.name
    table_format: Optional[str] = constants.INFO_SCHEMA_TABLE_FORMAT
    external_volume: Optional[str] = None
    file_format: Optional[str] = None
    cluster_by: Optional[str] = None
    automatic_clustering: Optional[bool] = False
    is_transient: Optional[bool] = False
    change_tracking: Optional[str] = None


class InfoSchemaCatalogIntegration(CatalogIntegration):
    catalog_name = constants.DEFAULT_INFO_SCHEMA_CATALOG.name
    catalog_type = constants.DEFAULT_INFO_SCHEMA_CATALOG.catalog_type
    table_format = constants.INFO_SCHEMA_TABLE_FORMAT
    allows_writes = True
    change_tracking = None

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        # we overwrite this because the base provides too much config
        self.name: str = config.name
        self.external_volume: Optional[str] = config.external_volume
        if adapter_properties := config.adapter_properties:
            self.change_tracking = adapter_properties.get(
                SnowflakeIcebergTableRelationParameters.change_tracking
            )

    def build_relation(self, model: RelationConfig) -> InfoSchemaCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        return InfoSchemaCatalogRelation(
            cluster_by=parse_model.cluster_by(model),
            automatic_clustering=parse_model.automatic_clustering(model),
            is_transient=parse_model.is_transient(model),
            change_tracking=resolve_change_tracking(model, self.change_tracking),
        )
