from dataclasses import dataclass
from typing import Optional, Dict, Any, List, TYPE_CHECKING

from dbt.adapters.relation_configs import RelationConfigChange, RelationResults
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.contracts.relation import ComponentName
from typing_extensions import Self

from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase

if TYPE_CHECKING:
    import agate


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeHybridTableConfig(SnowflakeRelationConfigBase):
    """
    This config follows the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table

    The following parameters are configurable by dbt:
    - name: name of the hybrid table
    - schema_name: schema containing the table
    - database_name: database containing the table
    - columns: dictionary of column definitions with types
    - primary_key: list of columns forming the primary key (required)
    - indexes: list of secondary index definitions
    - unique_key: list of columns with unique constraints
    - foreign_keys: list of foreign key constraint definitions
    - query: the SQL query for CTAS
    """

    name: str
    schema_name: str
    database_name: str
    columns: Dict[str, str]  # column_name: data_type
    primary_key: List[str]
    query: Optional[str] = None
    indexes: Optional[List[Dict[str, Any]]] = None
    unique_key: Optional[List[str]] = None
    foreign_keys: Optional[List[Dict[str, Any]]] = None

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
            "columns": config_dict.get("columns", {}),
            "primary_key": config_dict.get("primary_key", []),
            "query": config_dict.get("query"),
            "indexes": config_dict.get("indexes"),
            "unique_key": config_dict.get("unique_key"),
            "foreign_keys": config_dict.get("foreign_keys"),
        }

        return super().from_dict(kwargs_dict)  # type:ignore

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        """Parse a RelationConfig into a dictionary for SnowflakeHybridTableConfig"""
        config_dict = {
            "name": relation_config.identifier,
            "schema_name": relation_config.schema,
            "database_name": relation_config.database,
            "query": relation_config.compiled_code,
            "columns": relation_config.config.extra.get("columns", {}),  # type:ignore
            "primary_key": relation_config.config.extra.get("primary_key", []),  # type:ignore
            "indexes": relation_config.config.extra.get("indexes"),  # type:ignore
            "unique_key": relation_config.config.extra.get("unique_key"),  # type:ignore
            "foreign_keys": relation_config.config.extra.get("foreign_keys"),  # type:ignore
        }

        # Handle primary_key as string or list
        if isinstance(config_dict["primary_key"], str):
            config_dict["primary_key"] = [config_dict["primary_key"]]

        # Handle unique_key as string or list
        if isinstance(config_dict.get("unique_key"), str):
            config_dict["unique_key"] = [config_dict["unique_key"]]

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        """Parse results from SHOW HYBRID TABLES and SHOW INDEXES"""
        hybrid_table: "agate.Row" = relation_results["hybrid_table"].rows[0]

        config_dict = {
            "name": hybrid_table.get("name"),
            "schema_name": hybrid_table.get("schema_name"),
            "database_name": hybrid_table.get("database_name"),
            "query": hybrid_table.get("text"),
            "columns": hybrid_table.get("columns", {}),
            "primary_key": hybrid_table.get("primary_key", []),
            "indexes": relation_results.get("indexes"),
            "unique_key": hybrid_table.get("unique_key"),
            "foreign_keys": hybrid_table.get("foreign_keys"),
        }

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeHybridTablePrimaryKeyConfigChange(RelationConfigChange):
    context: Optional[List[str]] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeHybridTableIndexConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeHybridTableColumnConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass
class SnowflakeHybridTableConfigChangeset:
    primary_key: Optional[SnowflakeHybridTablePrimaryKeyConfigChange] = None
    indexes: Optional[SnowflakeHybridTableIndexConfigChange] = None
    columns: Optional[SnowflakeHybridTableColumnConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            [
                self.primary_key.requires_full_refresh if self.primary_key else False,
                self.indexes.requires_full_refresh if self.indexes else False,
                self.columns.requires_full_refresh if self.columns else False,
            ]
        )

    @property
    def has_changes(self) -> bool:
        return any([self.primary_key, self.indexes, self.columns])
