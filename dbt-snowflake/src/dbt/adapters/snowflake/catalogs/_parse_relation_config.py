from typing import Iterable, Optional

from dbt_common.exceptions import DbtConfigError
from dbt.adapters.contracts.relation import RelationConfig


def external_volume(model: RelationConfig) -> Optional[str]:
    return model.config.get("external_volume")


def base_location(model: RelationConfig) -> str:
    config = model.config
    prefix = config.get("base_location_root") or "_dbt"  # allow users to pass in None
    subpath = config.get("base_location_subpath")

    path = f"{prefix}/{model.schema}/{model.identifier}"
    if subpath:
        path += f"/{subpath}"

    return path


def cluster_by(model: RelationConfig) -> Optional[str]:
    fields = model.config.get("cluster_by")
    if isinstance(fields, Iterable):
        return ", ".join(fields)
    elif isinstance(fields, str):
        return fields
    elif fields is not None:
        raise DbtConfigError(f"Unexpected cluster_by configuration: {fields}")
    return None


def automatic_clustering(model: RelationConfig) -> bool:
    return model.config.get("automatic_clustering", False)
