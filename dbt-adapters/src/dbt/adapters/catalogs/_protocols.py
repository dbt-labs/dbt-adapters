from typing import Any, Dict, Protocol


class _TableFormatLike(Protocol):
    value: str


class CatalogV2(Protocol):
    """Structural interface for CatalogV2 (defined in dbt-core).

    Defined here as a Protocol to avoid a circular dependency. dbt-adapters
    uses this for typing bridge_v2_catalog and its hook methods.
    """

    name: str
    catalog_type: str
    table_format: _TableFormatLike
    config: Dict[str, Dict[str, Any]]
