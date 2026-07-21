from typing import Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.snowflake.constants import SnowflakeIcebergTableRelationParameters

logger = AdapterLogger("Snowflake")

_BOOL_TO_STR_MAP = {
    True: "TRUE",
    False: "FALSE",
    "true": "TRUE",
    "false": "FALSE",
}


def resolve_change_tracking(
    model: RelationConfig,
    integration_default: Optional[bool | str],
    is_iceberg: bool = False,
) -> Optional[str]:
    """
    Resolves change tracking for a catalog integration. The model config overrides the
    integration default; if neither is set, returns None.

    Snowflake does not allow change tracking to be turned off for Iceberg tables, so when
    `is_iceberg` and the resolved value is "FALSE", we warn and omit the property (return None)
    rather than emitting DDL that Snowflake would reject.
    """
    if (
        model.config
        and (
            change_tracking := model.config.get(
                SnowflakeIcebergTableRelationParameters.change_tracking, integration_default
            )
        )
        is not None
    ):
        if isinstance(change_tracking, str):
            change_tracking = change_tracking.lower()
        try:
            resolved = _BOOL_TO_STR_MAP[change_tracking]
        except KeyError:
            raise ValueError("Invalid value for change_tracking. Expected 'true' or 'false'.")

        if is_iceberg and resolved == "FALSE":
            logger.warning(
                "Change tracking cannot be turned off for Iceberg tables in Snowflake. "
                "Ignoring change_tracking=false; the property will be omitted and Snowflake "
                "will keep change tracking enabled."
            )
            return None
        return resolved
    else:
        return None
