import textwrap

from dataclasses import dataclass, field
from typing import FrozenSet, Optional, Type, Iterator, Tuple


from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import ComponentName, RelationConfig
from dbt.adapters.events.types import AdapterEventDebug
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.adapters.utils import classproperty
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.events.functions import fire_event

from dbt.adapters.snowflake.relation_configs import (
    RefreshMode,
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableRefreshModeConfigChange,
    SnowflakeDynamicTableTargetLagConfigChange,
    SnowflakeDynamicTableWarehouseConfigChange,
    TableFormat,
    SnowflakeQuotePolicy,
    SnowflakeRelationType,
)


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    type: Optional[SnowflakeRelationType] = None
    table_format: str = TableFormat.DEFAULT
    quote_policy: SnowflakeQuotePolicy = field(default_factory=lambda: SnowflakeQuotePolicy())
    require_alias: bool = False
    relation_configs = {
        SnowflakeRelationType.DynamicTable: SnowflakeDynamicTableConfig,
    }
    renameable_relations: FrozenSet[SnowflakeRelationType] = field(
        default_factory=lambda: frozenset(
            {
                SnowflakeRelationType.Table,  # type: ignore
                SnowflakeRelationType.View,  # type: ignore
            }
        )
    )

    replaceable_relations: FrozenSet[SnowflakeRelationType] = field(
        default_factory=lambda: frozenset(
            {
                SnowflakeRelationType.DynamicTable,  # type: ignore
                SnowflakeRelationType.Table,  # type: ignore
                SnowflakeRelationType.View,  # type: ignore
            }
        )
    )

    @property
    def is_dynamic_table(self) -> bool:
        return self.type == SnowflakeRelationType.DynamicTable

    @property
    def is_iceberg_format(self) -> bool:
        return self.table_format == TableFormat.ICEBERG

    @classmethod
    def is_transient(cls, model: RelationConfig) -> bool:
        """
        Always supply transient on table create DDL unless user specifically sets
        transient to false or unset.

        Args:
            model (RelationConfig): `config.model` (not `model`) from the jinja context.

        Returns:
            True if the user has set it to True or if the user has explicitly unset it.
        """
        if not model.config:
            return False
        if model.config.get("table_format") == "iceberg":
            return False
        return model.config.get("transient", False) or model.config.get("transient", True)

    @classproperty
    def DynamicTable(cls) -> str:
        return str(SnowflakeRelationType.DynamicTable)

    @classproperty
    def get_relation_type(cls) -> Type[SnowflakeRelationType]:
        return SnowflakeRelationType

    @classmethod
    def from_config(cls, config: RelationConfig) -> RelationConfigBase:
        relation_type: str = config.config.materialized  # type:ignore

        if relation_config := cls.relation_configs.get(relation_type):
            return relation_config.from_relation_config(config)

        raise DbtRuntimeError(
            f"from_config() is not supported for the provided relation type: {relation_type}"
        )

    @classmethod
    def dynamic_table_config_changeset(
        cls, relation_results: RelationResults, relation_config: RelationConfig
    ) -> Optional[SnowflakeDynamicTableConfigChangeset]:
        existing_dynamic_table = SnowflakeDynamicTableConfig.from_relation_results(
            relation_results
        )
        new_dynamic_table = SnowflakeDynamicTableConfig.from_relation_config(relation_config)

        config_change_collection = SnowflakeDynamicTableConfigChangeset()

        if new_dynamic_table.target_lag != existing_dynamic_table.target_lag:
            config_change_collection.target_lag = SnowflakeDynamicTableTargetLagConfigChange(
                action=RelationConfigChangeAction.alter,  # type:ignore
                context=new_dynamic_table.target_lag,
            )

        if new_dynamic_table.snowflake_warehouse != existing_dynamic_table.snowflake_warehouse:
            config_change_collection.snowflake_warehouse = (
                SnowflakeDynamicTableWarehouseConfigChange(
                    action=RelationConfigChangeAction.alter,  # type:ignore
                    context=new_dynamic_table.snowflake_warehouse,
                )
            )

        if (
            new_dynamic_table.refresh_mode != RefreshMode.AUTO
            and new_dynamic_table.refresh_mode != existing_dynamic_table.refresh_mode
        ):
            config_change_collection.refresh_mode = SnowflakeDynamicTableRefreshModeConfigChange(
                action=RelationConfigChangeAction.create,  # type:ignore
                context=new_dynamic_table.refresh_mode,
            )

        if config_change_collection.has_changes:
            return config_change_collection
        return None

    def as_case_sensitive(self) -> "SnowflakeRelation":
        path_part_map = {}

        for path in ComponentName:  # type:ignore
            if self.include_policy.get_part(path):
                part = self.path.get_part(path)
                if part:
                    if self.quote_policy.get_part(path):
                        path_part_map[path] = part
                    else:
                        path_part_map[path] = part.upper()

        return self.replace_path(**path_part_map)

    @property
    def can_be_renamed(self) -> bool:
        """
        Standard tables and dynamic tables can be renamed, but Snowflake does not support renaming iceberg relations.
        The iceberg standard does support renaming, so this may change in the future.
        """
        return self.type in self.renameable_relations and not self.is_iceberg_format

    def get_ddl_prefix_for_alter(self) -> str:
        """All ALTER statements on Iceberg tables require an ICEBERG prefix"""
        if self.is_iceberg_format or self.catalog == "snowflake":
            return "iceberg"
        else:
            return ""

    def get_iceberg_ddl_options(self, config: RelationConfig) -> str:
        # If the base_location_root config is supplied, overwrite the default value ("_dbt/")
        base_location: str = (
            f"{config.get('base_location_root', '_dbt')}/{self.schema}/{self.name}"  # type:ignore
        )

        if subpath := config.get("base_location_subpath"):  # type:ignore
            base_location += f"/{subpath}"

        external_volume = config.get("external_volume")  # type:ignore
        iceberg_ddl_predicates: str = f"""
        external_volume = '{external_volume}'
        catalog = 'snowflake'
        base_location = '{base_location}'
        """
        return textwrap.indent(textwrap.dedent(iceberg_ddl_predicates), " " * 10)

    def __drop_conditions(self, old_relation: "SnowflakeRelation") -> Iterator[Tuple[bool, str]]:
        drop_view_message: str = (
            f"Dropping relation {old_relation} because it is a view and target relation {self} "
            f"is of type {self.type}."
        )

        drop_table_for_iceberg_message: str = (
            f"Dropping relation {old_relation} because it is a default format table "
            f"and target relation {self} is an Iceberg format table."
        )

        drop_iceberg_for_table_message: str = (
            f"Dropping relation {old_relation} because it is an Iceberg format table "
            f"and target relation {self} is a default format table."
        )

        # An existing view must be dropped for model to build into a table".
        yield (not old_relation.is_table, drop_view_message)
        # An existing table must be dropped for model to build into an Iceberg table.
        yield (
            old_relation.is_table
            and not old_relation.is_iceberg_format
            and self.is_iceberg_format,
            drop_table_for_iceberg_message,
        )
        # existing Iceberg table must be dropped for model to build into a table.
        yield (
            old_relation.is_table
            and old_relation.is_iceberg_format
            and not self.is_iceberg_format,
            drop_iceberg_for_table_message,
        )

    def needs_to_drop(self, old_relation: Optional["SnowflakeRelation"]) -> bool:
        """
        To convert between Iceberg and non-Iceberg relations, a preemptive drop is
        required.

        drops cause latency, but it should be a relatively infrequent occurrence.

        Some Boolean expression below are logically redundant, but this is done for easier
        readability.
        """

        if old_relation is None:
            return False

        for condition, message in self.__drop_conditions(old_relation):
            if condition:
                fire_event(AdapterEventDebug(base_msg=message))
                return True

        return False
