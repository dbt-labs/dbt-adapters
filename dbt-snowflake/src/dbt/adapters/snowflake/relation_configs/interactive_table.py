from dataclasses import dataclass
from typing import Optional

from dbt_common.exceptions import CompilationError
from dbt_common.events.functions import warn_or_error

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.events.types import AdapterEventWarning
from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationResults,
)

from dbt.adapters.snowflake import parse_model
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase
from dbt.adapters.snowflake.relation_configs._normalize import (
    ABSENT as _ABSENT,
    normalize_cluster_by as _normalize_cluster_by,
    normalize_target_lag as _normalize_target_lag,
    normalize_warehouse as _normalize_warehouse,
    absent_to_none as _absent_to_none,
    non_blank as _non_blank,
)

# The exact `SHOW INTERACTIVE TABLES` select list, in order. `refresh_warehouse` is not the dynamic-table `warehouse` column -- different SHOW command, don't unify.
INTERACTIVE_TABLE_COLUMNS = (
    "name",
    "schema_name",
    "database_name",
    "text",
    "target_lag",
    "refresh_warehouse",
    "initialization_warehouse",
    "cluster_by",
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableConfig(SnowflakeRelationConfigBase):
    """Configuration for a Snowflake interactive table.

    `SHOW INTERACTIVE TABLES` exposes only `cluster_by`, `target_lag`,
    `refresh_warehouse`, and `initialization_warehouse` -- note `refresh_warehouse`,
    NOT the `warehouse` column that `SHOW DYNAMIC TABLES` returns. The rewritten
    `text` column is unusable for diffing and is deliberately not read.
    """

    name: Optional[str] = None
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    query: Optional[str] = None
    cluster_by: Optional[str] = None
    target_lag: Optional[str] = None
    snowflake_warehouse: Optional[str] = None
    refresh_warehouse: Optional[str] = None
    snowflake_initialization_warehouse: Optional[str] = None

    # --- normalized views, for COMPARISON ONLY -------------------------------
    # These never replace the stored values: DDL needs the user's exact text.

    @property
    def cluster_by_normalized(self) -> Optional[str]:
        return _normalize_cluster_by(self.cluster_by)

    @property
    def target_lag_normalized(self) -> Optional[str]:
        return _normalize_target_lag(self.target_lag)

    @property
    def refresh_warehouse_normalized(self) -> Optional[str]:
        return _normalize_warehouse(self.refresh_warehouse)

    @property
    def warehouse_parameter(self) -> Optional[str]:
        """The refresh warehouse this config would use, if dynamic.

        `refresh_warehouse` wins when set; otherwise `snowflake_warehouse` serves
        both roles, as for dynamic tables. Only meaningful when `is_dynamic` --
        Snowflake rejects a warehouse on a static table.
        """
        return _non_blank(self.refresh_warehouse) or _non_blank(self.snowflake_warehouse)

    @property
    def warehouse_parameter_normalized(self) -> Optional[str]:
        return _normalize_warehouse(self.warehouse_parameter)

    @property
    def snowflake_initialization_warehouse_normalized(self) -> Optional[str]:
        return _normalize_warehouse(self.snowflake_initialization_warehouse)

    @property
    def is_dynamic(self) -> bool:
        """Must use the normalized lag: the literal string 'none' means no lag and must read as static."""
        return self.target_lag_normalized is not None

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> dict:
        extra = relation_config.config.extra if relation_config.config else {}

        cluster_by = parse_model.cluster_by(relation_config)
        # Checked per element, not on the joined string: `["id", "  "]` joins to
        # `"id,   "`, which is truthy but renders `cluster by (id,   )` (001003).
        raw_cluster_by = (
            relation_config.config.get("cluster_by") if relation_config.config else None
        )
        keys = [raw_cluster_by] if isinstance(raw_cluster_by, str) else list(raw_cluster_by or [])
        if not keys or any(not str(key).strip() for key in keys):
            raise CompilationError(
                f"interactive_table models require `cluster_by` to name at least one "
                f"non-blank column; `CREATE INTERACTIVE TABLE` without `CLUSTER BY`, or "
                f"with only blank entries, is rejected by Snowflake (010405): "
                f"{relation_config.identifier}"
            )

        if str(extra.get("table_format", "")).strip().casefold() == "iceberg":
            raise CompilationError(
                f"Interactive tables do not support `table_format: iceberg`: "
                f"{relation_config.identifier}"
            )

        if extra.get("transient"):
            raise CompilationError(
                f"transient=true is not supported for interactive_table models; "
                f"`TRANSIENT INTERACTIVE TABLE` is a Snowflake syntax error (001003). "
                f"Set `transient: false` on this model to override an inherited value: "
                f"{relation_config.identifier}"
            )

        target_lag = extra.get("target_lag")
        warehouse = _non_blank(extra.get("refresh_warehouse")) or _non_blank(
            extra.get("snowflake_warehouse")
        )
        if target_lag and str(target_lag).strip().casefold() not in _ABSENT and not warehouse:
            raise CompilationError(
                f"target_lag requires refresh_warehouse or snowflake_warehouse to be set "
                f"for interactive_table models (010412): {relation_config.identifier}"
            )

        # Collapsed on the config side too: a clear written as the `NONE` literal
        # must reach the alter macro as absent, or it emits `set ... = NONE`.
        initialization_warehouse = _absent_to_none(extra.get("snowflake_initialization_warehouse"))
        is_dynamic = bool(target_lag) and str(target_lag).strip().casefold() not in _ABSENT
        # Inert rather than fatal: `ddl_body.sql` drops the value before any DDL when the
        # table is static. No matching warning for `snowflake_warehouse` -- that one still
        # selects the warehouse the build runs on, so it isn't inert.
        if not is_dynamic and initialization_warehouse:
            warn_or_error(
                AdapterEventWarning(
                    base_msg=(
                        "snowflake_initialization_warehouse is ignored on an interactive "
                        "table without target_lag; it only applies when the table "
                        "self-refreshes."
                    )
                )
            )

        return {
            "name": relation_config.identifier,
            "schema_name": relation_config.schema,
            "database_name": relation_config.database,
            "query": relation_config.compiled_code,
            "cluster_by": cluster_by,
            "target_lag": target_lag,
            "snowflake_warehouse": extra.get("snowflake_warehouse"),
            "refresh_warehouse": extra.get("refresh_warehouse"),
            "snowflake_initialization_warehouse": initialization_warehouse,
        }

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        row = cls._get_first_row(relation_results["interactive_table"])

        def get(column: str) -> Optional[str]:
            # Absent column, NULL cell, and no-such-relation all read as None here.
            value = row.get(column)
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        return {
            "name": get("name"),
            "schema_name": get("schema_name"),
            "database_name": get("database_name"),
            "cluster_by": get("cluster_by"),
            "target_lag": get("target_lag"),
            "refresh_warehouse": get("refresh_warehouse"),
            # Collapsed at load time: the alter macro's `unset` branch keys on falsiness.
            "snowflake_initialization_warehouse": _absent_to_none(get("initialization_warehouse")),
        }


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableTargetLagConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        # Only a value-to-value change is alterable via `ALTER INTERACTIVE TABLE
        # ... SET TARGET_LAG`. Both transitions must rebuild: unsetting a lag
        # (dynamic -> static) is rejected with "invalid value 'null' for
        # property 'TARGET_LAG'" (001422); setting one on an already-static
        # table (static -> dynamic) is rejected with "invalid property
        # 'TARGET_LAG' for 'TABLE'" (001420).
        return self.action != RelationConfigChangeAction.alter


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableClusterByConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        # DIVERGES from dynamic tables (which return False and emit
        # `ALTER DYNAMIC TABLE ... CLUSTER BY`). Snowflake rejects
        # `ALTER ... CLUSTER BY` on an interactive table with 001003.
        return True


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableRefreshWarehouseConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeInteractiveTableInitializationWarehouseConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass
class SnowflakeInteractiveTableConfigChangeset:
    target_lag: Optional[SnowflakeInteractiveTableTargetLagConfigChange] = None
    cluster_by: Optional[SnowflakeInteractiveTableClusterByConfigChange] = None
    refresh_warehouse: Optional[SnowflakeInteractiveTableRefreshWarehouseConfigChange] = None
    snowflake_initialization_warehouse: Optional[
        SnowflakeInteractiveTableInitializationWarehouseConfigChange
    ] = None

    @property
    def _changes(self) -> list:
        return [
            self.target_lag,
            self.cluster_by,
            self.refresh_warehouse,
            self.snowflake_initialization_warehouse,
        ]

    @property
    def requires_full_refresh(self) -> bool:
        return any(change.requires_full_refresh for change in self._changes if change)

    @property
    def has_changes(self) -> bool:
        return any(change is not None for change in self._changes)
