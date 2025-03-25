from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationMode
from dbt.adapters.contracts.relation import RelationConfig


@dataclass
class IcebergManagedCatalogRelation:
    base_location: str
    external_volume: Optional[str] = None


class IcebergManagedCatalogIntegration(CatalogIntegration):
    allows_writes = CatalogIntegrationMode.WRITE

    def catalog_relation(self, config: RelationConfig) -> IcebergManagedCatalogRelation:
        return IcebergManagedCatalogRelation(
            base_location=self._base_location(config),
            external_volume=self._external_volume(config),
        )

    def _external_volume(self, config: RelationConfig) -> Optional[str]:
        return config.config.extra.get("external_volume", self.external_volume)

    @staticmethod
    def _base_location(config: RelationConfig) -> str:
        # If the base_location_root config is supplied, overwrite the default value ("_dbt/")
        prefix = config.config.extra.get("base_location_root", "_dbt")

        base_location = f"{prefix}/{config.schema}/{config.identifier}"

        if subpath := config.config.extra.get("base_location_subpath"):
            base_location += f"/{subpath}"

        return base_location
