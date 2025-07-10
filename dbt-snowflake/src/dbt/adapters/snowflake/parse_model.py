from typing import Iterable, Optional

from dbt_common.exceptions import DbtConfigError

from dbt.adapters.catalogs import CATALOG_INTEGRATION_MODEL_CONFIG_NAME
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake import constants
from dbt.adapters.snowflake.constants import SnowflakeIcebergTableRelationParameters


def max_data_extension_time_in_days(model: RelationConfig) -> Optional[int]:
    return (
        model.config.get(
            SnowflakeIcebergTableRelationParameters.max_data_extension_time_in_days, False
        )
        if model.config
        else None
    )


def automatic_clustering(model: RelationConfig) -> Optional[bool]:
    return (
        model.config.get(SnowflakeIcebergTableRelationParameters.automatic_clustering, False)
        if model.config
        else None
    )


def base_location(model: RelationConfig) -> Optional[str]:
    if not model.config:
        return None

    prefix = (
        model.config.get("base_location_root") or "_dbt"
    )  # use "_dbt" even when users pass in None
    path = f"{prefix}/{model.schema}/{model.identifier}"
    if subpath := model.config.get("base_location_subpath"):
        path += f"/{subpath}"

    return path


def catalog_name(model: RelationConfig) -> Optional[str]:
    if not model.config:
        return None

    if _catalog := model.config.get(CATALOG_INTEGRATION_MODEL_CONFIG_NAME):
        # make catalog_name case-insensitive
        return _catalog.upper()

    _table_format = table_format(model)
    if _table_format == constants.ICEBERG_TABLE_FORMAT:
        return constants.DEFAULT_BUILT_IN_CATALOG.name

    return constants.DEFAULT_INFO_SCHEMA_CATALOG.name


def cluster_by(model: RelationConfig) -> Optional[str]:
    if not model.config:
        return None

    fields = model.config.get("cluster_by")
    if isinstance(fields, str):
        return fields
    if isinstance(fields, Iterable):
        return ", ".join(fields)
    if fields is not None:
        raise DbtConfigError(f"Unexpected cluster_by configuration: {fields}")
    return None


def external_volume(model: RelationConfig) -> Optional[str]:
    return model.config.get("external_volume") if model.config else None


def is_transient(model: RelationConfig) -> Optional[bool]:
    """
    Always supply transient on table create DDL unless user specifically sets
    transient to false or unset.

    Args:
        model (RelationConfig): `config.model` (not `model`) from the jinja context.

    Returns:
        None if there is no materialized config on the model; this shouldn't happen
        False if we know this is an iceberg table format (excludes format set by the catalog)
        True if the user has set it to True or if the user has explicitly unset it
        False otherwise
    """
    if not model.config:
        return None

    if table_format(model) == constants.ICEBERG_TABLE_FORMAT:
        return False
    return model.config.get("transient", False) or model.config.get("transient", True)


def table_format(model: RelationConfig) -> Optional[str]:
    if not model.config:
        return None

    # we don't know what the table format is if it's not on the model
    # this could be derived from the catalog and will be derived from the catalog moving forward
    # so we cannot default to INFO_SCHEMA here
    if _table_format := model.config.get("table_format"):
        # make table_format case-insensitive
        return _table_format.upper()
    return None
