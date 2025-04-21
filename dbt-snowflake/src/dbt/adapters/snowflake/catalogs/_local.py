from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
)
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake import constants, parse_model


@dataclass
class LocalCatalogRelation:
    catalog_type: str = constants.DEFAULT_LOCAL_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_LOCAL_CATALOG.name
    table_format: Optional[str] = constants.LOCAL_TABLE_FORMAT
    external_volume: Optional[str] = None
    cluster_by: Optional[str] = None
    automatic_clustering: Optional[bool] = False
    is_transient: Optional[bool] = False


class LocalCatalogIntegration(CatalogIntegration):
    catalog_name = constants.DEFAULT_LOCAL_CATALOG.name
    catalog_type = constants.DEFAULT_LOCAL_CATALOG.catalog_type
    table_format = constants.LOCAL_TABLE_FORMAT
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        # we overwrite this because the base provides too much config
        self.name: str = config.name
        self.external_volume: Optional[str] = config.external_volume

    def build_relation(self, model: RelationConfig) -> LocalCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        return LocalCatalogRelation(
            cluster_by=parse_model.cluster_by(model),
            automatic_clustering=parse_model.automatic_clustering(model),
            is_transient=parse_model.is_transient(model),
        )
