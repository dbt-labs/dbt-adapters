from typing import Optional

from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.snowflake.constants import SnowflakeIcebergTableRelationParameters

_BOOL_TO_STR_MAP = {
    True: "TRUE",
    False: "FALSE",
    "true": "TRUE",
    "false": "FALSE",
}


def resolve_change_tracking(
    model: RelationConfig, integration_default: Optional[str]
) -> Optional[str]:
    """
    Resolves change tracking for a catalog integration.
    If `change_tracking` is set in the model config, it will override the integration default.
    If not set on either the model or integration, it will return None.
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
            return _BOOL_TO_STR_MAP[change_tracking]
        except KeyError:
            raise ValueError("Invalid value for change_tracking. Expected 'true' or 'false'.")
    else:
        return None
