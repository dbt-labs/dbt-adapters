from typing import Any, Optional

from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.catalogs import CATALOG_INTEGRATION_MODEL_CONFIG_NAME
from dbt.adapters.bigquery import constants


def _config_get(config: Any, key: str) -> Any:
    """Read ``key`` from a node config that may not be dict-like.

    Some node configs are typed, non-mapping objects (e.g. a saved-query export's
    ``ExportConfig``, which has no ``.get``). Return None for those rather than
    raising AttributeError.
    """
    get = getattr(config, "get", None)
    return get(key) if callable(get) else None


def catalog_name(model: RelationConfig) -> Optional[str]:
    # while this looks equivalent to `if not getattr(model, "config", None):`, `mypy` disagrees
    if not hasattr(model, "config") or not model.config:
        return None

    if _catalog := _config_get(model.config, CATALOG_INTEGRATION_MODEL_CONFIG_NAME):
        return _catalog

    # TODO: deprecate this as it's been replaced with catalog_name
    if _catalog := _config_get(model.config, "catalog"):
        return _catalog

    return constants.DEFAULT_INFO_SCHEMA_CATALOG.name
