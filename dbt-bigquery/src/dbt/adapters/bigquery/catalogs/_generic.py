from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import (
    CatalogIntegration,
)
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.bigquery import constants, parse_model


@dataclass
class BigQueryCatalogRelation:
    catalog_type: str = constants.DEFAULT_INFO_SCHEMA_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_INFO_SCHEMA_CATALOG.name
    table_format: Optional[str] = constants.INFO_SCHEMA_TABLE_FORMAT
    file_format: Optional[str] = constants.INFO_SCHEMA_FILE_FORMAT
    external_volume: Optional[str] = None

    @property
    def storage_uri(self) -> Optional[str]:
        return self.external_volume

    @storage_uri.setter
    def storage_uri(self, value: Optional[str]) -> None:
        self.external_volume = value


class BigQueryCatalogIntegration(CatalogIntegration):
    catalog_type = constants.GENERIC_CATALOG_TYPE
    allows_writes = True

    @property
    def storage_uri(self) -> Optional[str]:
        return self.external_volume

    @storage_uri.setter
    def storage_uri(self, value: Optional[str]) -> None:
        self.external_volume = value

    def build_relation(self, model: RelationConfig) -> BigQueryCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        return BigQueryCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.catalog_name,
            table_format=self.table_format,
            file_format=self.file_format,
            external_volume=parse_model.storage_uri(model) or self.external_volume,
        )
