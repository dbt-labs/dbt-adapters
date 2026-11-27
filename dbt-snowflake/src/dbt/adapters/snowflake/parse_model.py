from typing import Any, Dict, Iterable, List, Optional, Union

from dbt_common.exceptions import DbtConfigError

from dbt.adapters.catalogs import CATALOG_INTEGRATION_MODEL_CONFIG_NAME
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake import constants
from dbt.adapters.snowflake.constants import SnowflakeIcebergTableRelationParameters


def auto_refresh(model: RelationConfig) -> Optional[bool]:
    return model.config.get("auto_refresh") if model.config else None


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

    # Suppress base_location when SNOWFLAKE_MANAGED is explicitly set in model config.
    # The "no external_volume" case is handled upstream in _built_in.py where the
    # fully-resolved external_volume (including catalog integration defaults) is known.
    ev = model.config.get("external_volume")
    if ev and ev.upper() == "SNOWFLAKE_MANAGED":
        return None

    prefix = (
        model.config.get("base_location_root") or "_dbt"
    )  # use "_dbt" even when users pass in None
    path = f"{prefix}/{model.schema}/{model.identifier}"
    if subpath := model.config.get("base_location_subpath"):
        path += f"/{subpath}"

    return path


def catalog_name(model: RelationConfig) -> Optional[str]:
    if not model.config or not hasattr(model.config, "get"):
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


# Keys may have to be lowercased due to Glue
def partition_by(model: RelationConfig) -> Optional[Union[str, List[str]]]:
    if not model.config:
        return None

    fields = model.config.get("partition_by")
    if isinstance(fields, str):
        return fields
    if isinstance(fields, Iterable):
        return list(fields)
    if fields is not None:
        raise DbtConfigError(f"Unexpected partition_by configuration: {fields}")
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


def iceberg_version(model: RelationConfig) -> Optional[int]:
    return (
        model.config.get(SnowflakeIcebergTableRelationParameters.iceberg_version)
        if model.config
        else None
    )


def target_file_size(model: RelationConfig) -> Optional[str]:
    return model.config.get("target_file_size") if model.config else None


def data_metric_schedule(model: RelationConfig) -> Optional[str]:
    """The evaluation schedule for the relation's data metric functions.

    Passed through to Snowflake verbatim (for example ``5 MINUTE``,
    ``USING CRON 0 6 * * * UTC`` or ``TRIGGER_ON_CHANGES``); Snowflake validates the
    value when the schedule is set.
    """
    if not model.config:
        return None

    schedule = model.config.get("data_metric_schedule")
    if schedule is None:
        return None
    if not isinstance(schedule, str):
        raise DbtConfigError(
            f"Unexpected data_metric_schedule configuration: expected a string, got {schedule!r}"
        )

    schedule = schedule.strip()
    return schedule or None


def data_metric_functions(model: RelationConfig) -> List[Dict[str, Any]]:
    """The data metric functions configured on the relation.

    Each entry is normalized to ``{"name": <fully qualified dmf>, "arguments": [<column>, ...]}``.
    A single ``expression`` string is wrapped into a one-element list so that a scalar
    and a list are handled the same way downstream.
    """
    if not model.config:
        return []

    configured = model.config.get("data_metric_functions")
    if not configured:
        return []
    if not isinstance(configured, list):
        raise DbtConfigError(
            "Unexpected data_metric_functions configuration: expected a list of "
            f"{{metric, expression}} mappings, got {configured!r}"
        )

    metric_functions: List[Dict[str, Any]] = []
    for entry in configured:
        if not isinstance(entry, dict):
            raise DbtConfigError(
                "Unexpected data_metric_functions entry: expected a mapping with "
                f"`metric` and `expression`, got {entry!r}"
            )

        metric = entry.get("metric")
        expression = entry.get("expression")
        if not metric or expression in (None, "", []):
            raise DbtConfigError(
                "Each data_metric_functions entry requires a non-empty `metric` and "
                f"`expression`, got {entry!r}"
            )

        if isinstance(expression, str):
            arguments = [expression]
        elif isinstance(expression, (list, tuple)):
            arguments = list(expression)
        else:
            raise DbtConfigError(
                "Unexpected data_metric_functions `expression`: expected a column name "
                f"or list of column names, got {expression!r}"
            )

        metric_functions.append({"name": metric, "arguments": arguments})

    return metric_functions
