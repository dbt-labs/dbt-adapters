from dataclasses import dataclass
from typing import Optional, Dict, Any, TYPE_CHECKING

from dbt.adapters.relation_configs import RelationConfigChange, RelationResults
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.contracts.relation import ComponentName
from dbt_common.exceptions import CompilationError
from typing_extensions import Self

from dbt.adapters.snowflake.parse_model import cluster_by
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase

if TYPE_CHECKING:
    import agate


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableConfig(SnowflakeRelationConfigBase):
    """
    This config follows the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-interactive-table

    The following parameters are configurable by dbt:
    - name: name of the interactive table
    - query: the query behind the table
    - cluster_by: specifies the columns to cluster on (required)
    - target_lag: the maximum lag for auto-refresh (optional, makes it a dynamic interactive table)
    - snowflake_warehouse: the standard warehouse for refresh operations (required when target_lag is set)

    There is no ALTER INTERACTIVE TABLE command; config changes require CREATE OR REPLACE.
    """

    name: str
    schema_name: str
    database_name: str
    query: str
    cluster_by: str
    target_lag: Optional[str] = None
    snowflake_warehouse: Optional[str] = None

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Self:
        kwargs_dict = {
            "name": cls._render_part(
                ComponentName.Identifier, config_dict.get("name")  # type:ignore
            ),
            "schema_name": cls._render_part(
                ComponentName.Schema, config_dict.get("schema_name")  # type:ignore
            ),
            "database_name": cls._render_part(
                ComponentName.Database, config_dict.get("database_name")  # type:ignore
            ),
            "query": config_dict.get("query"),
            "cluster_by": config_dict.get("cluster_by"),
            "target_lag": config_dict.get("target_lag"),
            "snowflake_warehouse": config_dict.get("snowflake_warehouse"),
        }

        return super().from_dict(kwargs_dict)  # type:ignore

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        cluster_by_value = cluster_by(relation_config)
        if not cluster_by_value:
            raise CompilationError(
                "Interactive tables require a `cluster_by` configuration. "
                "Please add `cluster_by` to your model config."
            )

        config_dict: Dict[str, Any] = {
            "name": relation_config.identifier,
            "schema_name": relation_config.schema,
            "database_name": relation_config.database,
            "query": relation_config.compiled_code,
            "cluster_by": cluster_by_value,
            "target_lag": relation_config.config.extra.get("target_lag"),  # type:ignore
            "snowflake_warehouse": relation_config.config.extra.get(  # type:ignore
                "snowflake_warehouse"
            ),
        }

        target_lag = config_dict.get("target_lag")
        if target_lag and not config_dict.get("snowflake_warehouse"):
            raise CompilationError(
                "Interactive tables with `target_lag` require a `snowflake_warehouse` "
                "configuration for refresh operations."
            )

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        interactive_table: "agate.Row" = relation_results["interactive_table"].rows[0]

        cluster_by_raw = interactive_table.get("cluster_by")
        if cluster_by_raw is not None and str(cluster_by_raw).strip() not in ("", "NONE", "None"):
            cluster_by_val = str(cluster_by_raw).strip()
        else:
            raise CompilationError(
                "Interactive table metadata is missing a valid `cluster_by` value. "
                "This is unexpected — interactive tables require cluster_by."
            )

        config_dict = {
            "name": interactive_table.get("name"),
            "schema_name": interactive_table.get("schema_name"),
            "database_name": interactive_table.get("database_name"),
            "query": interactive_table.get("text"),
            "cluster_by": cluster_by_val,
            "target_lag": interactive_table.get("target_lag"),
            "snowflake_warehouse": interactive_table.get("warehouse"),
        }

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableClusterByConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableTargetLagConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableWarehouseConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass
class SnowflakeInteractiveTableConfigChangeset:
    cluster_by: Optional[SnowflakeInteractiveTableClusterByConfigChange] = None
    target_lag: Optional[SnowflakeInteractiveTableTargetLagConfigChange] = None
    snowflake_warehouse: Optional[SnowflakeInteractiveTableWarehouseConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            [
                self.cluster_by.requires_full_refresh if self.cluster_by else False,
                self.target_lag.requires_full_refresh if self.target_lag else False,
                (
                    self.snowflake_warehouse.requires_full_refresh
                    if self.snowflake_warehouse
                    else False
                ),
            ]
        )

    @property
    def has_changes(self) -> bool:
        return any(
            [
                self.cluster_by,
                self.target_lag,
                self.snowflake_warehouse,
            ]
        )
