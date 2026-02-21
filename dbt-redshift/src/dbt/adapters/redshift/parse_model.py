from typing import List, Optional

from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.catalogs import CATALOG_INTEGRATION_MODEL_CONFIG_NAME


def catalog_name(model: RelationConfig) -> Optional[str]:
    """
    Get catalog name from model config.

    Args:
        model: RelationConfig from the jinja context (config.model)

    Returns:
        The catalog name if specified, otherwise None (indicating standard Redshift table)
    """
    if not hasattr(model, "config") or not model.config:
        return None

    # Check for catalog_name (preferred) or catalog (legacy)
    if _catalog := model.config.get(CATALOG_INTEGRATION_MODEL_CONFIG_NAME):
        return _catalog

    if _catalog := model.config.get("catalog"):
        return _catalog

    # Return None to indicate no catalog is specified
    # This allows the adapter to use standard Redshift tables
    return None


def external_schema(model: RelationConfig) -> Optional[str]:
    """
    Get external schema name from model config.

    This is the Redshift external schema that points to the Glue Data Catalog database.

    Args:
        model: RelationConfig from the jinja context (config.model)

    Returns:
        The external schema name if specified, otherwise None
    """
    if not hasattr(model, "config") or not model.config:
        return None

    return model.config.get("external_schema")


def partition_by(model: RelationConfig) -> Optional[List[str]]:
    """
    Get partition columns from model config.

    Args:
        model: RelationConfig from the jinja context (config.model)

    Returns:
        List of partition column names, or None if not specified
    """
    if not hasattr(model, "config") or not model.config:
        return None

    partition_config = model.config.get("partition_by")

    if partition_config is None:
        return None

    # Normalize to list
    if isinstance(partition_config, str):
        return [partition_config]

    return list(partition_config)


def storage_uri(model: RelationConfig) -> Optional[str]:
    """
    Get explicit storage URI from model config.

    This allows users to override the auto-generated S3 path.

    Args:
        model: RelationConfig from the jinja context (config.model)

    Returns:
        The storage URI if specified, otherwise None
    """
    if not hasattr(model, "config") or not model.config:
        return None

    return model.config.get("storage_uri")


def file_format(model: RelationConfig) -> Optional[str]:
    """
    Get file format from model config.

    Args:
        model: RelationConfig from the jinja context (config.model)

    Returns:
        The file format if specified, otherwise None (defaults to parquet)
    """
    if not hasattr(model, "config") or not model.config:
        return None

    return model.config.get("file_format")
