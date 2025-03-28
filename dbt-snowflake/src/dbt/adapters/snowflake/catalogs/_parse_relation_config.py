from typing import Any, Dict, Iterable, Optional

from dbt_common.exceptions import DbtConfigError


def external_volume(model: Dict[str, Any]) -> Optional[str]:
    """
    Args:
        model: A RelationConfig object as a dict
    """
    return model["config"].get("external_volume")


def base_location(model: Dict[str, Any]) -> str:
    """
    Args:
        model: A RelationConfig object as a dict
    """
    schema = model["schema"]
    identifier = model["alias"]
    config = model["config"]
    prefix = config.get("base_location_root", "_dbt")
    subpath = config.get("base_location_subpath")

    path = f"{prefix}/{schema}/{identifier}"
    if subpath:
        path += f"/{subpath}"

    return path


def cluster_by(model: Dict[str, Any]) -> Optional[str]:
    """
    Args:
        model: A RelationConfig object as a dict
    """
    fields = model["config"].get("cluster_by")
    if isinstance(fields, Iterable):
        return ", ".join(fields)
    elif isinstance(fields, str):
        return fields
    elif fields is not None:
        raise DbtConfigError(f"Unexpected cluster_by configuration: {fields}")
    return None


def automatic_clustering(model: Dict[str, Any]) -> bool:
    """
    Args:
        model: A RelationConfig object as a dict
    """
    return model["config"].get("automatic_clustering", False)
