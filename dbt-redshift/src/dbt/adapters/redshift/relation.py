from dataclasses import dataclass, field
from dbt.adapters.contracts.relation import RelationConfig
from typing import FrozenSet, Optional

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.adapters.base import RelationType
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation_configs import (
    RedshiftMaterializedViewConfig,
    RedshiftMaterializedViewConfigChangeset,
    RedshiftAutoRefreshConfigChange,
    RedshiftDistConfigChange,
    RedshiftSortConfigChange,
    RedshiftIncludePolicy,
    RedshiftQuotePolicy,
    MAX_CHARACTERS_IN_IDENTIFIER,
)


@dataclass(frozen=True, eq=False, repr=False)
class RedshiftRelation(BaseRelation):
    include_policy = RedshiftIncludePolicy  # type: ignore
    quote_policy = RedshiftQuotePolicy  # type: ignore
    require_alias: bool = False
    relation_configs = {
        RelationType.MaterializedView.value: RedshiftMaterializedViewConfig,  # type:ignore
    }
    renameable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,  # type:ignore
                RelationType.Table,  # type:ignore
            }
        )
    )
    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,  # type:ignore
            }
        )
    )

    def __post_init__(self):
        # Check for length of Redshift table/view names.
        # Check self.type to exclude test relation identifiers
        if (
            self.identifier is not None
            and self.type is not None
            and len(self.identifier) > MAX_CHARACTERS_IN_IDENTIFIER
        ):
            raise DbtRuntimeError(
                f"Relation name '{self.identifier}' "
                f"is longer than {MAX_CHARACTERS_IN_IDENTIFIER} characters"
            )

    def relation_max_name_length(self):
        return MAX_CHARACTERS_IN_IDENTIFIER

    @classmethod
    def from_config(cls, config: RelationConfig) -> RelationConfigBase:
        relation_type: str = config.config.materialized  # type: ignore

        if relation_config := cls.relation_configs.get(relation_type):
            return relation_config.from_relation_config(config)

        raise DbtRuntimeError(
            f"from_config() is not supported for the provided relation type: {relation_type}"
        )

    @classmethod
    def materialized_view_config_changeset(
        cls, relation_results: RelationResults, relation_config: RelationConfig
    ) -> Optional[RedshiftMaterializedViewConfigChangeset]:
        config_change_collection = RedshiftMaterializedViewConfigChangeset()

        existing_materialized_view = RedshiftMaterializedViewConfig.from_relation_results(
            relation_results
        )
        new_materialized_view = RedshiftMaterializedViewConfig.from_relation_config(
            relation_config
        )

        if new_materialized_view.autorefresh != existing_materialized_view.autorefresh:
            config_change_collection.autorefresh = RedshiftAutoRefreshConfigChange(
                action=RelationConfigChangeAction.alter,  # type:ignore
                context=new_materialized_view.autorefresh,
            )

        if new_materialized_view.dist != existing_materialized_view.dist:
            config_change_collection.dist = RedshiftDistConfigChange(
                action=RelationConfigChangeAction.alter,  # type:ignore
                context=new_materialized_view.dist,
            )

        if new_materialized_view.sort != existing_materialized_view.sort:
            config_change_collection.sort = RedshiftSortConfigChange(
                action=RelationConfigChangeAction.alter,  # type:ignore
                context=new_materialized_view.sort,
            )

        if config_change_collection.has_changes:
            return config_change_collection
        return None
