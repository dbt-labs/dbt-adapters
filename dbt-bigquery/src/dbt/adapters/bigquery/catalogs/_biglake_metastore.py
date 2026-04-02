from typing import Optional

from dbt.adapters.catalogs import CatalogIntegration
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.bigquery import constants
from dbt.adapters.bigquery.catalogs._relation import BigQueryCatalogRelation


class BigLakeCatalogIntegration(CatalogIntegration):
    catalog_type = constants.BIGLAKE_CATALOG_TYPE
    allows_writes = True

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
            external_volume=self._get_external_volume(model),
            storage_uri=self._calculate_storage_uri(model),
        )

    def _get_external_volume(self, model: RelationConfig) -> Optional[str]:
        """Model-level external_volume takes precedence over integration-level (default)."""
        model_external_volume = model.config.get("external_volume") if model.config else None
        return model_external_volume or self.external_volume

    def _calculate_storage_uri(self, model: RelationConfig) -> Optional[str]:
        if not model.config:
            return None

        if model_storage_uri := model.config.get("storage_uri"):
            return model_storage_uri

        external_volume = self._get_external_volume(model)
        if not external_volume:
            return None

        prefix = model.config.get("base_location_root") or "_dbt"
        storage_uri = f"{external_volume}/{prefix}/{model.schema}/{model.name}"
        if suffix := model.config.get("base_location_subpath"):
            storage_uri = f"{storage_uri}/{suffix}"
        return storage_uri
