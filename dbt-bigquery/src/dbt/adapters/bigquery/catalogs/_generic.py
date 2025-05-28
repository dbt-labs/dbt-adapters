from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogRelation,
)
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.bigquery import constants


@dataclass
class BigQueryCatalogRelation(CatalogRelation):
    catalog_type: str = constants.DEFAULT_INFO_SCHEMA_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_INFO_SCHEMA_CATALOG.name
    table_format: Optional[str] = constants.INFO_SCHEMA_TABLE_FORMAT
    file_format: Optional[str] = constants.INFO_SCHEMA_FILE_FORMAT
    external_volume: Optional[str] = None
    storage_uri: Optional[str] = None


class BigQueryCatalogIntegration(CatalogIntegration):
    catalog_type = constants.GENERIC_CATALOG_TYPE
    allows_writes = True

    def build_relation(self, model: RelationConfig) -> BigQueryCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        if model.config and (model_conf_storage_uri := model.config.get("storage_uri")):
            storage_uri = model_conf_storage_uri
            external_volume = None

        else:
            storage_uri = self._calculate_storage_uri(model)
            external_volume = self.external_volume

        return BigQueryCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.catalog_name,
            table_format=self.table_format,
            file_format=self.file_format,
            external_volume=external_volume,
            storage_uri=storage_uri,
        )

    def _calculate_storage_uri(self, model: RelationConfig) -> Optional[str]:
        if not model.config:
            return None
        storage_uri_base = ""
        if self.external_volume:
            storage_uri_base += self.external_volume
        location_root = (
            base_location_root
            if (base_location_root := model.config.get("base_location_root"))
            else "dbt"
        )
        storage_uri_base += f"/{location_root}"
        return f"{storage_uri_base}/{model.schema}/{model.name}"
