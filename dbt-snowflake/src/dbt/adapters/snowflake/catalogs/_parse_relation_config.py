from typing import Iterable, Optional

from dbt_common.exceptions import DbtConfigError
from dbt.adapters.contracts.relation import RelationConfig


def external_volume(model: RelationConfig) -> Optional[str]:
    if model.config:
        return model.config.get("external_volume")
    return None


def base_location(model: RelationConfig) -> str:
    if config := model.config:
        prefix = config.get("base_location_root") or "_dbt"  # allow users to pass in None
        subpath = config.get("base_location_subpath")
    else:
        prefix = "_dbt"
        subpath = None

    path = f"{prefix}/{model.schema}/{model.identifier}"
    if subpath:
        path += f"/{subpath}"

    return path


def cluster_by(model: RelationConfig) -> Optional[str]:
    if not model.config:
        return None

    fields = model.config.get("cluster_by")
    if isinstance(fields, Iterable):
        return ", ".join(fields)
    elif isinstance(fields, str):
        return fields
    elif fields is not None:
        raise DbtConfigError(f"Unexpected cluster_by configuration: {fields}")
    return None


def automatic_clustering(model: RelationConfig) -> bool:
    if model.config:
        return model.config.get("automatic_clustering", False)
    return False


def catalog_table(model: RelationConfig) -> str:
    if model.config:
        return model.config.get("catalog_table", model.identifier)
    return model.identifier


def catalog_namespace(model: RelationConfig) -> Optional[str]:
    if model.config:
        return model.config.get("catalog_namespace")
    return None


def replace_invalid_characters(model: RelationConfig) -> Optional[bool]:
    if model.config:
        return model.config.get("replace_invalid_characters")
    return None


def auto_refresh(model: RelationConfig) -> Optional[bool]:
    if model.config:
        return model.config.get("auto_refresh")
    return None
