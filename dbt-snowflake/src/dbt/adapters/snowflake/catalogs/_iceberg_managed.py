from dataclasses import dataclass
from typing import Iterable, Optional

from dbt.adapters.catalogs import CatalogIntegration
from dbt.adapters.contracts.relation import RelationConfig


@dataclass
class IcebergManagedCatalogRelation:
    base_location: str
    external_volume: Optional[str] = None
    catalog_name: str = "snowflake"
    table_format: str = "iceberg"
    cluster_by: Optional[str] = None
    automatic_clustering: bool = False


class IcebergManagedCatalogIntegration(CatalogIntegration):
    allows_writes = True

    def build_relation(self, config: RelationConfig) -> IcebergManagedCatalogRelation:
        return IcebergManagedCatalogRelation(
            base_location=self.__base_location(config),
            external_volume=config.config.extra.get("external_volume", self.external_volume),
            cluster_by=self.__cluster_by(config),
            automatic_clustering=config.config.get("automatic_clustering", False),
        )

    @staticmethod
    def __base_location(config: RelationConfig) -> str:
        # If the base_location_root config is supplied, overwrite the default value ("_dbt/")
        prefix = config.config.extra.get("base_location_root", "_dbt")

        base_location = f"{prefix}/{config.schema}/{config.identifier}"

        if subpath := config.config.extra.get("base_location_subpath"):
            base_location += f"/{subpath}"

        return base_location

    @staticmethod
    def __cluster_by(config: RelationConfig) -> Optional[str]:
        if cluster_by := config.config.get("cluster_by"):
            if isinstance(cluster_by, Iterable):
                return ", ".join(cluster_by)
            return cluster_by
        return None
