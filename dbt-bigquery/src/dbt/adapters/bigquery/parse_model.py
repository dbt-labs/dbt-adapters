from typing import Optional

from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.catalogs import CATALOG_INTEGRATION_MODEL_CONFIG_NAME
from dbt.adapters.bigquery import constants


def catalog_name(model: RelationConfig) -> Optional[str]:
    # while this looks equivalent to `if not getattr(model, "config", None):`, `mypy` disagrees
    if not hasattr(model, "config") or not model.config:
        return None

    if _catalog := model.config.get(CATALOG_INTEGRATION_MODEL_CONFIG_NAME):
        return _catalog

    # TODO: deprecate this as it's been replaced with catalog_name
    if _catalog := model.config.get("catalog"):
        return _catalog

    return constants.DEFAULT_INFO_SCHEMA_CATALOG.name
