from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake.catalogs._parse_relation_config import (
    automatic_clustering,
    base_location,
    cluster_by,
    external_volume,
)


@dataclass
class IcebergManagedCatalogRelation:
    base_location: str
    catalog_name: str = "snowflake"
    table_format: str = "iceberg"
    external_volume: Optional[str] = None
    cluster_by: Optional[str] = None
    automatic_clustering: bool = False


class IcebergManagedCatalogIntegration(CatalogIntegration):
    catalog_name = "snowflake"
    catalog_type = "iceberg_managed"
    table_format = "iceberg"
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        # we overwrite this because the base provides too much config
        self.name: str = config.name
        self.external_volume: Optional[str] = config.external_volume

    def build_relation(self, config: RelationConfig) -> IcebergManagedCatalogRelation:
        return IcebergManagedCatalogRelation(
            base_location=base_location(config),
            external_volume=external_volume(config) or self.external_volume,
            cluster_by=cluster_by(config),
            automatic_clustering=automatic_clustering(config),
        )
