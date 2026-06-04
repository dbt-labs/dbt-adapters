from typing import Any, Dict, Iterable, List, Optional, Union

from dbt_common.exceptions import DbtConfigError

from dbt.adapters.contracts.relation import RelationConfig


def table_format(model: RelationConfig) -> Optional[str]:
    if not model.config:
        return None

    # we don't know what the table format is if it's not on the model;
    # for catalog-driven models this is derived from the catalog integration
    if _table_format := model.config.get("table_format"):
        # make table_format case-insensitive
        return _table_format.lower()
    return None


def partition_by(model: RelationConfig) -> Optional[Union[str, List[str]]]:
    """Iceberg partition spec: a single transform string or a list of them.

    Redshift supports identity, bucket[N], truncate[W], year, month, day, hour.
    """
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
    """The S3 ``LOCATION`` prefix the Iceberg table data is written to."""
    if not model.config:
        return None
    return model.config.get("external_volume") or model.config.get("location")


def table_properties(model: RelationConfig) -> Optional[Dict[str, Any]]:
    """Iceberg ``TABLE PROPERTIES`` (Redshift currently supports ``compression_type``)."""
    if not model.config:
        return None
    return model.config.get("table_properties")
