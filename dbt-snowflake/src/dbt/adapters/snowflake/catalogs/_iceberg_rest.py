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
    file_format: Optional[str] = None
    target_file_size: Optional[str] = None
    max_data_extension_time_in_days: Optional[int] = None
    auto_refresh: Optional[bool] = None


class IcebergRestCatalogIntegration(CatalogIntegration):
    catalog_type = constants.DEFAULT_ICEBERG_REST_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_ICEBERG_REST_CATALOG.name
    table_format = constants.ICEBERG_TABLE_FORMAT
    file_format = None  # Snowflake chooses based on stage-format
    allows_writes = True
    auto_refresh = None
    catalog_linked_database: Optional[str] = None
    max_data_extension_time_in_days: Optional[int] = None
    target_file_size: Optional[str] = None

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        # we overwrite this because the base provides too much config
        self.name: str = config.name
        self.external_volume: Optional[str] = config.external_volume
        if adapter_properties := config.adapter_properties:
            self.catalog_linked_database = adapter_properties.get("catalog_linked_database")
            self.auto_refresh = adapter_properties.get("auto_refresh")
            self.target_file_size = adapter_properties.get("target_file_size")
            self.max_data_extension_time_in_days = adapter_properties.get(
                "max_data_extension_time_in_days"
            )

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

        if parse_model.external_volume(model):
            raise InvalidRelationConfigError(
                model,
                model.config,
                "external_volume is not supported for iceberg rest catalog integrations with catalog linked databases",
            )

        return IcebergRestCatalogRelation(
            catalog_name=self.catalog_name,
            external_volume=None,
            catalog_linked_database=self.catalog_linked_database,
            auto_refresh=parse_model.auto_refresh(model) or self.auto_refresh,
            target_file_size=parse_model.target_file_size(model) or self.target_file_size,
            max_data_extension_time_in_days=parse_model.max_data_extension_time_in_days(model)
            or self.max_data_extension_time_in_days,
        )
