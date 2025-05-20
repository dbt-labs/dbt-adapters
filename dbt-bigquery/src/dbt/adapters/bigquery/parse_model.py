from typing import Optional

from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.bigquery import constants


def catalog_name(model: RelationConfig) -> Optional[str]:
    if not model.config:
        return None

    if _catalog := model.config.get("catalog"):
        return _catalog

    return constants.DEFAULT_INFO_SCHEMA_CATALOG.name


def storage_uri(model: RelationConfig) -> Optional[str]:
    return model.config.get("storage_uri") if model.config else None
