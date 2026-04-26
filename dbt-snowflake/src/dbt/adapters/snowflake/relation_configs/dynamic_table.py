from dataclasses import dataclass
from typing import Optional, Dict, Any, TYPE_CHECKING, Union

from dbt.adapters.relation_configs import RelationConfigChange, RelationResults
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.contracts.relation import ComponentName
from dbt_common.dataclass_schema import StrEnum  # doesn't exist in standard library until py3.11
from dbt_common.exceptions import CompilationError
from typing_extensions import Self

from dbt.adapters.snowflake.parse_model import cluster_by
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase

if TYPE_CHECKING:
    import agate


class RefreshMode(StrEnum):
    AUTO = "AUTO"
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"

    @classmethod
    def default(cls) -> Self:
        return cls("AUTO")


class Initialize(StrEnum):
    ON_CREATE = "ON_CREATE"
    ON_SCHEDULE = "ON_SCHEDULE"

    @classmethod
    def default(cls) -> Self:
        return cls("ON_CREATE")


class Scheduler(StrEnum):
    ENABLE = "ENABLE"
    DISABLE = "DISABLE"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableConfig(SnowflakeRelationConfigBase):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    The following parameters are configurable by dbt:
    - name: name of the dynamic table
    - query: the query behind the table
    - target_lag: the maximum amount of time that the dynamic table’s content should lag behind updates to the base tables
    - snowflake_warehouse: the name of the warehouse used to execute DDL (CREATE/ALTER); also used as the dynamic table's refresh warehouse when refresh_warehouse is not set
    - refresh_warehouse: when set, used as the WAREHOUSE = parameter in the DDL (the table's self-refresh warehouse); snowflake_warehouse still executes the DDL
    - snowflake_initialization_warehouse: the name of the warehouse used for the initializations and reinitializations of the dynamic table
    - refresh_mode: specifies the refresh type for the dynamic table
    - initialize: specifies the behavior of the initial refresh of the dynamic table
    - scheduler: specifies whether to ENABLE or DISABLE the dynamic table's scheduler
    - cluster_by: specifies the columns to cluster on
    - immutable_where: specifies an immutability constraint expression
    - transient: specifies whether the dynamic table is transient (no fail-safe). snowflake_default_transient_dynamic_tables determines the default value

    There are currently no non-configurable parameters.
    """

    name: str
    schema_name: str
    database_name: str
    query: str
    snowflake_warehouse: str
    target_lag: Optional[str] = None
    snowflake_initialization_warehouse: Optional[str] = None
    refresh_warehouse: Optional[str] = None
    refresh_mode: Optional[RefreshMode] = RefreshMode.default()
    initialize: Optional[Initialize] = Initialize.default()
    scheduler: Optional[Scheduler] = None
    row_access_policy: Optional[str] = None
    table_tag: Optional[str] = None
    cluster_by: Optional[Union[str, list[str]]] = None
    copy_grants: Optional[bool] = None
    immutable_where: Optional[str] = None
    transient: Optional[bool] = None

    @property
    def warehouse_parameter(self) -> str:
        """The value used for the WAREHOUSE = parameter in CREATE/REPLACE DYNAMIC TABLE DDL.

        When refresh_warehouse is set it is used here, so the dynamic table's self-refresh
        runs on a different warehouse than the one executing the DDL (snowflake_warehouse).
        When only snowflake_warehouse is set it serves both roles, preserving existing behaviour.
        """
        return self.refresh_warehouse or self.snowflake_warehouse

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
            "target_lag": config_dict.get("target_lag"),
            "snowflake_warehouse": config_dict.get("snowflake_warehouse"),
            "snowflake_initialization_warehouse": config_dict.get(
                "snowflake_initialization_warehouse"
            ),
            "refresh_warehouse": config_dict.get("refresh_warehouse"),
            "refresh_mode": config_dict.get("refresh_mode"),
            "initialize": config_dict.get("initialize"),
            "scheduler": config_dict.get("scheduler"),
            "row_access_policy": config_dict.get("row_access_policy"),
            "table_tag": config_dict.get("table_tag"),
            "cluster_by": config_dict.get("cluster_by"),
            "copy_grants": config_dict.get("copy_grants"),
            "immutable_where": config_dict.get("immutable_where"),
            "transient": config_dict.get("transient"),
        }

        return super().from_dict(kwargs_dict)  # type:ignore

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        config_dict = {
            "name": relation_config.identifier,
            "schema_name": relation_config.schema,
            "database_name": relation_config.database,
            "query": relation_config.compiled_code,
            "target_lag": relation_config.config.extra.get("target_lag"),  # type:ignore
            "snowflake_warehouse": relation_config.config.extra.get(  # type:ignore
                "snowflake_warehouse"
            ),
            "snowflake_initialization_warehouse": relation_config.config.extra.get(  # type:ignore
                "snowflake_initialization_warehouse"
            ),
            "refresh_warehouse": relation_config.config.extra.get(  # type:ignore
                "refresh_warehouse"
            ),
            "row_access_policy": relation_config.config.extra.get(  # type:ignore
                "row_access_policy"
            ),
            "table_tag": relation_config.config.extra.get("table_tag"),  # type:ignore
            "cluster_by": cluster_by(relation_config),
            "copy_grants": relation_config.config.extra.get("copy_grants"),  # type:ignore
            "immutable_where": relation_config.config.extra.get(  # type:ignore
                "immutable_where"
            ),
            "transient": relation_config.config.extra.get("transient"),  # type:ignore
        }

        if refresh_mode := relation_config.config.extra.get("refresh_mode"):  # type:ignore
            config_dict["refresh_mode"] = refresh_mode.upper()

        if initialize := relation_config.config.extra.get("initialize"):  # type:ignore
            config_dict["initialize"] = initialize.upper()

        target_lag = config_dict.get("target_lag")
        if scheduler := relation_config.config.extra.get("scheduler"):  # type:ignore
            normalized_scheduler = scheduler.upper()
            if normalized_scheduler not in (Scheduler.ENABLE, Scheduler.DISABLE):
                raise CompilationError(
                    "Invalid value for `scheduler`: "
                    f"'{scheduler}'. Expected one of: "
                    f"{Scheduler.ENABLE}, {Scheduler.DISABLE}."
                )

            if normalized_scheduler == Scheduler.ENABLE and target_lag is None:
                raise CompilationError(
                    "Invalid dynamic table config: `scheduler='ENABLE'` requires `target_lag`."
                )

            if normalized_scheduler == Scheduler.DISABLE and target_lag is not None:
                raise CompilationError(
                    "Invalid dynamic table config: `scheduler='DISABLE'` requires `target_lag` "
                    "to be omitted."
                )
            config_dict["scheduler"] = normalized_scheduler
        elif target_lag:
            config_dict["scheduler"] = Scheduler.ENABLE
        else:
            config_dict["scheduler"] = Scheduler.DISABLE

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        dynamic_table: "agate.Row" = relation_results["dynamic_table"].rows[0]

        # Snowflake returns "NONE" as a string for unset optional warehouse values
        # Some Snowflake environments may also return empty strings
        # We need to convert these to Python None to avoid rendering invalid SQL
        init_warehouse = dynamic_table.get("initialization_warehouse")
        if init_warehouse is not None and (
            str(init_warehouse).upper() == "NONE" or str(init_warehouse).strip() == ""
        ):
            init_warehouse = None

        # Snowflake returns immutable_where as "IMMUTABLE WHERE (expression)"
        # We need to extract just the expression to match what users configure
        immutable_where = dynamic_table.get("immutable_where")
        if immutable_where is not None:
            immutable_where_str = str(immutable_where).strip()
            if immutable_where_str.upper() == "NONE" or immutable_where_str == "":
                immutable_where = None
            elif immutable_where_str.upper().startswith("IMMUTABLE WHERE ("):
                # Strip "IMMUTABLE WHERE (" prefix and ")" suffix
                immutable_where = immutable_where_str[17:-1]  # len("IMMUTABLE WHERE (") = 17

        # Snowflake may return empty string for unset cluster_by — normalize to None.
        # When all cluster_by elements are plain column references, Snowflake wraps
        # the entire expression with LINEAR(...) as the default clustering method
        # (e.g. "id" → "LINEAR(id)", "id, value" → "LINEAR(id, value)").
        # Strip the wrapper so the value matches the user's config.
        cluster_by = dynamic_table.get("cluster_by")
        if cluster_by is not None and str(cluster_by).strip() not in ("", "NONE", "None"):
            cluster_by = str(cluster_by).strip()
            if cluster_by.upper().startswith("LINEAR(") and cluster_by.endswith(")"):
                cluster_by = cluster_by[7:-1]
        else:
            cluster_by = None

        scheduler = dynamic_table.get("scheduler")
        target_lag = dynamic_table.get("target_lag")
        if scheduler is None:
            scheduler = Scheduler.ENABLE if target_lag else Scheduler.DISABLE

        config_dict = {
            "name": dynamic_table.get("name"),
            "schema_name": dynamic_table.get("schema_name"),
            "database_name": dynamic_table.get("database_name"),
            "query": dynamic_table.get("text"),
            "target_lag": target_lag,
            "snowflake_warehouse": dynamic_table.get("warehouse"),
            "snowflake_initialization_warehouse": init_warehouse,
            "refresh_mode": dynamic_table.get("refresh_mode"),
            "scheduler": scheduler,
            "row_access_policy": dynamic_table.get("row_access_policy"),
            "table_tag": dynamic_table.get("table_tag"),
            "cluster_by": cluster_by,
            "immutable_where": immutable_where,
            # agate.Row.get() returns None when the column is absent, which is the
            # correct default -- it means "not queried" and skips transient comparison.
            "transient": dynamic_table.get("transient"),
            # we don't get initialize since that's a one-time scheduler attribute, not a DT attribute
        }

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableTargetLagConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableWarehouseConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableInitializationWarehouseConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableRefreshModeConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableImmutableWhereConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableClusterByConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableSchedulerConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableTransientConfigChange(RelationConfigChange):
    context: Optional[bool] = None

    @property
    def requires_full_refresh(self) -> bool:
        # Transient cannot be changed via ALTER, requires full table recreation
        return True


@dataclass
class SnowflakeDynamicTableConfigChangeset:
    target_lag: Optional[SnowflakeDynamicTableTargetLagConfigChange] = None
    snowflake_warehouse: Optional[SnowflakeDynamicTableWarehouseConfigChange] = None
    snowflake_initialization_warehouse: Optional[
        SnowflakeDynamicTableInitializationWarehouseConfigChange
    ] = None
    refresh_mode: Optional[SnowflakeDynamicTableRefreshModeConfigChange] = None
    scheduler: Optional[SnowflakeDynamicTableSchedulerConfigChange] = None
    immutable_where: Optional[SnowflakeDynamicTableImmutableWhereConfigChange] = None
    cluster_by: Optional[SnowflakeDynamicTableClusterByConfigChange] = None
    transient: Optional[SnowflakeDynamicTableTransientConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            [
                self.target_lag.requires_full_refresh if self.target_lag else False,
                (
                    self.snowflake_warehouse.requires_full_refresh
                    if self.snowflake_warehouse
                    else False
                ),
                (
                    self.snowflake_initialization_warehouse.requires_full_refresh
                    if self.snowflake_initialization_warehouse
                    else False
                ),
                self.refresh_mode.requires_full_refresh if self.refresh_mode else False,
                self.scheduler.requires_full_refresh if self.scheduler else False,
                self.immutable_where.requires_full_refresh if self.immutable_where else False,
                self.cluster_by.requires_full_refresh if self.cluster_by else False,
                self.transient.requires_full_refresh if self.transient else False,
            ]
        )

    @property
    def has_changes(self) -> bool:
        return any(
            [
                self.target_lag,
                self.snowflake_warehouse,
                self.snowflake_initialization_warehouse,
                self.refresh_mode,
                self.scheduler,
                self.immutable_where,
                self.cluster_by,
                self.transient,
            ]
        )
