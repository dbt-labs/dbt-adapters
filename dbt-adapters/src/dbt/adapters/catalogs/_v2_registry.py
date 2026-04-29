from typing import Dict, Optional, Tuple, Type

from dbt_common.dataclass_schema import dbtClassMixin

_REGISTRY: Dict[Tuple[str, str], Type[dbtClassMixin]] = {}


def register_catalog_config(
    catalog_type: str, platform: str, config_class: Type[dbtClassMixin]
) -> None:
    """Register a platform-specific catalog config schema for v2 catalogs.yml validation.

    Adapter packages call this on import to make their config schema available for
    parse-time structural and semantic validation by dbt-core. The provided class is
    expected to be a dbtClassMixin dataclass; core uses cls.validate() for jsonschema
    checks and cls.from_dict() to trigger __post_init__ semantic checks.

    Args:
        catalog_type: the v2 catalog type identifier (e.g. "horizon", "iceberg_rest").
        platform: the adapter platform (e.g. "snowflake", "databricks", "bigquery").
        config_class: a dbtClassMixin dataclass describing the platform's config schema.
    """
    _REGISTRY[(catalog_type, platform)] = config_class


def get_catalog_config(catalog_type: str, platform: str) -> Optional[Type[dbtClassMixin]]:
    """Look up a registered platform-specific catalog config schema.

    Returns None if no config has been registered for the given (catalog_type, platform)
    pair, signalling that the adapter does not yet support v2 catalogs of this type.
    """
    return _REGISTRY.get((catalog_type, platform))
