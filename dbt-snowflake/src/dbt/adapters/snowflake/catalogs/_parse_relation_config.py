from typing import Iterable, Optional

from dbt_common.exceptions import DbtConfigError
from dbt.adapters.contracts.relation import RelationConfig


def external_volume(config: RelationConfig) -> Optional[str]:
    if materialization_config := config.config:
        return materialization_config.extra.get("external_volume")
    return None


def base_location(config: RelationConfig) -> str:
    if materialization_config := config.config:
        prefix = materialization_config.extra.get("base_location_root", "_dbt")
        path = f"{prefix}/{config.schema}/{config.identifier}"
        if subpath := materialization_config.extra.get("base_location_subpath"):
            path += f"/{subpath}"
    else:
        path = f"_dbt/{config.schema}/{config.identifier}"
    return path


def cluster_by(config: RelationConfig) -> Optional[str]:
    if materialization_config := config.config:
        fields = materialization_config.extra.get("cluster_by")
        if isinstance(fields, Iterable):
            return ", ".join(fields)
        elif isinstance(fields, str):
            return fields
        elif fields is not None:
            raise DbtConfigError(f"Unexpected cluster_by configuration: {fields}")
    return None


def automatic_clustering(config: RelationConfig) -> bool:
    if materialization_config := config.config:
        return materialization_config.extra.get("automatic_clustering", False)
    return False
