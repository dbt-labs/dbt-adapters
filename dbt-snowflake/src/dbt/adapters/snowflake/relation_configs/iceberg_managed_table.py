from dataclasses import dataclass
from typing import Any, Dict, Optional

from dbt.adapters.contracts.relation import RelationConfig
from typing_extensions import Self

from dbt.adapters.snowflake.catalogs import SnowflakeCatalogRelation
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class IcebergManagedTableConfig(SnowflakeRelationConfigBase):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake

    This is currently bare bones, in order to support Snowflake's managed iceberg catalog.

    The following parameters are configurable by dbt:
    - catalog: the managed iceberg catalog and derived attributes, e.g. external volume and base location
    """

    external_volume: str
    base_location: str

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Self:
        catalog = config_dict["catalog"]
        kwargs_dict = {
            "external_volume": catalog.external_volume,
            "base_location": catalog.base_location,
        }
        return super().from_dict(kwargs_dict)  # type:ignore

    @classmethod
    def parse_relation_config(
        cls, relation_config: RelationConfig, catalog: Optional[SnowflakeCatalogRelation]
    ) -> Dict[str, Any]:
        return {"catalog": catalog}
