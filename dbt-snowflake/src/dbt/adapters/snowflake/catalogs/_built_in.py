from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
)
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake import constants, parse_model
from dbt.adapters.snowflake.constants import SnowflakeIcebergTableRelationParameters

_BOOL_TO_STR_MAP = {
    True: "TRUE",
    False: "FALSE",
    "true": "TRUE",
    "false": "FALSE",
}


def _validate_storage_serialization_policy(policy: str) -> None:
    if policy.upper() not in ["COMPATIBLE", "OPTIMIZED"]:
        raise ValueError(
            f"Invalid storage serialization policy: {policy}. Expected 'COMPATIBLE' or 'OPTIMIZED'."
        )


@dataclass
class BuiltInCatalogRelation:
    """
    Parameters representing configuration of a built-in Snowflake Iceberg table
    Ref: https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake#syntax

    Note that a lot of these parameters are optional, and the defaults for are inherited from the
    schema/database/account. To not add to the complexity of the user configuration, we do not set defaults for these
    parameters.
    """

    base_location: Optional[str]
    catalog_type: str = constants.DEFAULT_BUILT_IN_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_BUILT_IN_CATALOG.name
    table_format: Optional[str] = constants.ICEBERG_TABLE_FORMAT
    external_volume: Optional[str] = None
    file_format: Optional[str] = None
    cluster_by: Optional[str] = None
    automatic_clustering: Optional[bool] = False
    is_transient: Optional[bool] = False
    data_retention_time_in_days: Optional[int] = None
    max_data_extension_time_in_days: Optional[int] = None
    storage_serialization_policy: Optional[str] = None
    change_tracking: Optional[str] = None


class BuiltInCatalogIntegration(CatalogIntegration):
    catalog_name = constants.DEFAULT_BUILT_IN_CATALOG.name
    catalog_type = constants.DEFAULT_BUILT_IN_CATALOG.catalog_type
    table_format = constants.ICEBERG_TABLE_FORMAT
    file_format = None  # no file format for built-in catalogs
    allows_writes = True
    data_retention_time_in_days: Optional[int] = None
    max_data_extension_time_in_days: Optional[int] = None
    storage_serialization_policy = None
    change_tracking = None

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        # we overwrite this because the base provides too much config
        self.name: str = config.name
        self.external_volume: Optional[str] = config.external_volume
        if adapter_properties := config.adapter_properties:
            if storage_serialization_policy := adapter_properties.get(
                SnowflakeIcebergTableRelationParameters.storage_serialization_policy
            ):
                _validate_storage_serialization_policy(storage_serialization_policy)
                self.storage_serialization_policy = storage_serialization_policy

            self.max_data_extension_time_in_days = adapter_properties.get(
                SnowflakeIcebergTableRelationParameters.max_data_extension_time_in_days
            )

            self.change_tracking = adapter_properties.get(
                SnowflakeIcebergTableRelationParameters.change_tracking
            )

            self.data_retention_time_in_days = adapter_properties.get(
                SnowflakeIcebergTableRelationParameters.data_retention_time_in_days
            )

    def build_relation(self, model: RelationConfig) -> BuiltInCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """

        max_data_extension_time_in_days = (
            parse_model.max_data_extension_time_in_days(model)
            or self.max_data_extension_time_in_days
        )
        data_retention_time_in_days = (
            model.config.get(
                SnowflakeIcebergTableRelationParameters.data_retention_time_in_days,
                self.data_retention_time_in_days,
            )
            if model.config
            else self.data_retention_time_in_days
        )
        storage_serialization_policy = (
            model.config.get(
                SnowflakeIcebergTableRelationParameters.storage_serialization_policy,
                self.storage_serialization_policy,
            )
            if model.config
            else self.storage_serialization_policy
        )

        return BuiltInCatalogRelation(
            base_location=parse_model.base_location(model),
            external_volume=parse_model.external_volume(model) or self.external_volume,
            cluster_by=parse_model.cluster_by(model),
            automatic_clustering=parse_model.automatic_clustering(model),
            storage_serialization_policy=storage_serialization_policy,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            change_tracking=self._resolve_change_tracking(model),
            data_retention_time_in_days=data_retention_time_in_days,
        )

    def _resolve_change_tracking(self, model: RelationConfig) -> Optional[str]:
        """
        Resolves the change tracking for the catalog integration.
        If `change_tracking` is set in the model config, it will override the default.
        If 'change_tracking' is not set on either the model or integration, it will return None.
        """
        if (
            model.config
            and (
                change_tracking := model.config.get(
                    SnowflakeIcebergTableRelationParameters.change_tracking, self.change_tracking
                )
            )
            is not None
        ):
            if isinstance(change_tracking, str):
                change_tracking = change_tracking.lower()
            try:
                return _BOOL_TO_STR_MAP[change_tracking]
            except KeyError:
                raise ValueError("Invalid value for change_tracking. Expected 'true' or 'false'.")
        else:
            return None
