from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any, Optional, Protocol, Tuple, runtime_checkable

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.planning.create_from_query import PlanProvenance
from dbt.adapters.planning.incremental import (
    IncrementalMutationPlan,
    IncrementalPartitionFacts,
    IncrementalSchemaChangePlan,
)
from dbt.adapters.planning.materialization import (
    ExistingIndexStrategy,
    IncompatibleRelationStrategy,
    MaterializationHookStrategy,
    MaterializationStatementStrategy,
    MaterializationTransactionStrategy,
    SnapshotMaterializationFacts,
    SnapshotStrategyFacts,
    TableDocumentationStrategy,
    TableIndexStrategy,
    TableMaterializationFacts,
    ViewMaterializationFacts,
)


@dataclass(frozen=True)
class TableRelationFamily:
    """Relations participating in one table replacement."""

    target: BaseRelation
    existing: Optional[BaseRelation]
    intermediate: BaseRelation
    backup: BaseRelation
    preexisting_intermediate: Optional[BaseRelation]
    preexisting_backup: Optional[BaseRelation]


@dataclass(frozen=True)
class IncrementalRelationFamily(TableRelationFamily):
    """Relations participating in one incremental materialization."""

    staging: BaseRelation


@dataclass(frozen=True)
class SnapshotRelationFamily:
    """Relations participating in one snapshot materialization."""

    target: BaseRelation
    staging: BaseRelation
    target_exists: bool


class MaterializationConfig(Protocol):
    """Read-only model configuration exposed by dbt's runtime context."""

    def get(self, name: str, default: Any = None) -> Any: ...


@runtime_checkable
class MaterializationPlan(Protocol):
    """Any adapter-selected native materialization entry point."""

    provenance: Tuple[PlanProvenance, ...]

    def supports_native_execution(self, runtime: "MaterializationRuntime") -> bool: ...

    def execute(self, runtime: "MaterializationRuntime") -> BaseRelation: ...


@dataclass(frozen=True)
class IncrementalLifecycleFacts:
    """Complete live inputs used to resolve an incremental lifecycle."""

    table: TableMaterializationFacts
    schema_change: IncrementalSchemaChangePlan
    full_refresh: bool
    contract_enforced: bool
    partition: Optional[IncrementalPartitionFacts] = None

    def __post_init__(self) -> None:
        if not isinstance(self.table, TableMaterializationFacts):
            raise TypeError("Incremental lifecycle requires typed table facts")
        if not isinstance(self.schema_change, IncrementalSchemaChangePlan):
            raise TypeError("Incremental lifecycle requires a typed schema-change plan")
        if not isinstance(self.full_refresh, bool):
            raise TypeError(
                "Incremental lifecycle full-refresh state must be a boolean"
            )
        if not isinstance(self.contract_enforced, bool):
            raise TypeError("Incremental lifecycle contract state must be a boolean")
        if self.partition is not None and not isinstance(
            self.partition, IncrementalPartitionFacts
        ):
            raise TypeError("Incremental lifecycle partition facts must be typed")


@runtime_checkable
class MaterializationRuntime(Protocol):
    """dbt-core execution services available to semantic lifecycle strategies."""

    @property
    def config(self) -> MaterializationConfig: ...

    def table_relations(self) -> TableRelationFamily: ...

    def view_relations(self) -> TableRelationFamily: ...

    def resolve_table_strategy(
        self, strategy: "TableMaterializationStrategy"
    ) -> "TableMaterializationStrategy": ...

    def resolve_incremental_strategy(
        self, materialization: "IncrementalMaterializationPlan"
    ) -> "IncrementalMaterializationStrategy": ...

    def resolve_view_strategy(
        self, strategy: "ViewMaterializationStrategy"
    ) -> "ViewMaterializationStrategy": ...

    def resolve_snapshot_strategy(
        self, materialization: "SnapshotMaterializationPlan"
    ) -> "SnapshotMaterializationStrategy": ...

    def incremental_relations(
        self, mutation: IncrementalMutationPlan
    ) -> IncrementalRelationFamily: ...

    def snapshot_relations(self) -> SnapshotRelationFamily: ...

    def supports_snapshot_materialization(self) -> bool: ...

    def drop_if_exists(self, relation: Optional[BaseRelation]) -> None: ...

    def reload_relation(
        self, relation: Optional[BaseRelation]
    ) -> Optional[BaseRelation]: ...

    def run_hooks(self, phase: str, *, inside_transaction: bool) -> None: ...

    def create_from_query(
        self,
        relation: BaseRelation,
        *,
        temporary: bool = False,
        auto_begin: bool = True,
        query: Optional[str] = None,
    ) -> None: ...

    def create_view_from_query(
        self, relation: BaseRelation, *, auto_begin: bool = True
    ) -> None: ...

    def create_indexes(self, relation: BaseRelation) -> None: ...

    def drop_indexes(self, relation: BaseRelation) -> None: ...

    def rename(self, source: BaseRelation, destination: BaseRelation) -> None: ...

    def apply_grants(
        self,
        relation: BaseRelation,
        *,
        existing: Optional[BaseRelation],
        full_refresh: bool,
    ) -> None: ...

    def persist_docs(
        self, relation: BaseRelation, *, for_columns: bool = True
    ) -> None: ...

    def grant_view_access(self, relation: BaseRelation) -> None: ...

    def raise_wrong_relation_type(
        self, relation: BaseRelation, expected_type: str
    ) -> None: ...

    def build_snapshot_initial_query(self, strategy: SnapshotStrategyFacts) -> str: ...

    def build_snapshot_staging_query(
        self, strategy: SnapshotStrategyFacts, target: BaseRelation
    ) -> str: ...

    def validate_snapshot_target(
        self, target: BaseRelation, strategy: SnapshotStrategyFacts
    ) -> None: ...

    def check_snapshot_time_data_types(self, query: str) -> None: ...

    def reconcile_snapshot_columns(
        self,
        staging: BaseRelation,
        target: BaseRelation,
        strategy: SnapshotStrategyFacts,
    ) -> Sequence[str]: ...

    def execute_snapshot_merge(
        self,
        target: BaseRelation,
        staging: BaseRelation,
        insert_columns: Sequence[str],
    ) -> None: ...

    def post_snapshot(self, staging: BaseRelation) -> None: ...

    def commit(self) -> None: ...

    def invoke_setup(self, name: Optional[str]) -> Any: ...

    def invoke_teardown(self, name: Optional[str], context: Any) -> None: ...

    def expand_target_columns(
        self, source: BaseRelation, target: BaseRelation
    ) -> None: ...

    def process_schema_changes(
        self,
        plan: IncrementalSchemaChangePlan,
        source: BaseRelation,
        target: BaseRelation,
    ) -> Sequence[Any]: ...

    def execute_incremental_mutation(
        self,
        plan: IncrementalMutationPlan,
        relations: IncrementalRelationFamily,
        destination_columns: Sequence[Any],
        partition: Optional[IncrementalPartitionFacts],
    ) -> None: ...

    def insert_from_query(
        self, relation: BaseRelation, partition: IncrementalPartitionFacts
    ) -> None: ...

    def copy_incremental_partitions(
        self,
        source: BaseRelation,
        target: BaseRelation,
        partition: IncrementalPartitionFacts,
    ) -> None: ...


@runtime_checkable
class TableMaterializationStrategy(Protocol):
    provenance: Tuple[PlanProvenance, ...]
    facts: Optional[TableMaterializationFacts]

    def resolve(
        self, facts: TableMaterializationFacts
    ) -> "TableMaterializationStrategy": ...

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation: ...


@runtime_checkable
class ViewMaterializationStrategy(Protocol):
    provenance: Tuple[PlanProvenance, ...]
    facts: Optional[ViewMaterializationFacts]

    def resolve(
        self, facts: ViewMaterializationFacts
    ) -> "ViewMaterializationStrategy": ...

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation: ...


@dataclass(frozen=True)
class StageAndSwapTable:
    """Create a work relation, then atomically exchange it with the target."""

    indexes: TableIndexStrategy
    existing_indexes: ExistingIndexStrategy
    documentation: TableDocumentationStrategy
    transaction: MaterializationTransactionStrategy
    hooks: MaterializationHookStrategy
    statement: MaterializationStatementStrategy
    provenance: Tuple[PlanProvenance, ...]
    facts: Optional[TableMaterializationFacts] = None

    def __post_init__(self) -> None:
        if (
            self.documentation == TableDocumentationStrategy.AFTER_COMMIT
            and self.transaction != MaterializationTransactionStrategy.EXPLICIT_COMMIT
        ):
            raise ValueError(
                "Post-commit documentation requires explicit transaction control"
            )

    def resolve(self, facts: TableMaterializationFacts) -> "StageAndSwapTable":
        return replace(self, facts=facts)

    def supports_native_execution(self, runtime: MaterializationRuntime) -> bool:
        return True

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        if self.facts is None:
            return runtime.resolve_table_strategy(self).execute(runtime)

        relations = runtime.table_relations()
        runtime.drop_if_exists(relations.preexisting_intermediate)
        runtime.drop_if_exists(relations.preexisting_backup)
        if self.hooks == MaterializationHookStrategy.SPLIT:
            runtime.run_hooks("pre", inside_transaction=False)
        runtime.run_hooks("pre", inside_transaction=True)

        runtime.create_from_query(
            relations.intermediate,
            auto_begin=self.statement == MaterializationStatementStrategy.AUTO_BEGIN,
        )
        if self.indexes == TableIndexStrategy.BEFORE_SWAP:
            runtime.create_indexes(relations.intermediate)

        existing = runtime.reload_relation(relations.existing)
        if existing is not None:
            if self.existing_indexes == ExistingIndexStrategy.DROP_BEFORE_SWAP:
                runtime.drop_indexes(existing)
            assert self.facts.existing is not None
            if self.facts.existing.can_be_renamed:
                runtime.rename(existing, relations.backup)
            else:
                runtime.drop_if_exists(existing)

        runtime.rename(relations.intermediate, relations.target)
        if self.indexes == TableIndexStrategy.AFTER_SWAP:
            runtime.create_indexes(relations.target)
        runtime.run_hooks("post", inside_transaction=True)
        runtime.apply_grants(relations.target, existing=existing, full_refresh=True)

        if self.documentation == TableDocumentationStrategy.BEFORE_COMMIT:
            runtime.persist_docs(relations.target)
        if self.transaction == MaterializationTransactionStrategy.EXPLICIT_COMMIT:
            runtime.commit()
        if self.documentation == TableDocumentationStrategy.AFTER_COMMIT:
            runtime.persist_docs(relations.target)
            runtime.commit()

        runtime.drop_if_exists(relations.backup)
        if self.hooks == MaterializationHookStrategy.SPLIT:
            runtime.run_hooks("post", inside_transaction=False)
        return relations.target


@dataclass(frozen=True)
class DirectReplaceTable:
    """Replace the target in place using adapter-provided DDL semantics."""

    statement: MaterializationStatementStrategy
    provenance: Tuple[PlanProvenance, ...]
    setup_macro: Optional[str] = None
    teardown_macro: Optional[str] = None
    facts: Optional[TableMaterializationFacts] = None

    def __post_init__(self) -> None:
        if (self.setup_macro is None) != (self.teardown_macro is None):
            raise ValueError("Direct-replace setup and teardown macros must be paired")

    def resolve(self, facts: TableMaterializationFacts) -> "DirectReplaceTable":
        return replace(self, facts=facts)

    def supports_native_execution(self, runtime: MaterializationRuntime) -> bool:
        return True

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        if self.facts is None:
            return runtime.resolve_table_strategy(self).execute(runtime)

        relations = runtime.table_relations()
        envelope = runtime.invoke_setup(self.setup_macro)
        runtime.run_hooks("pre", inside_transaction=True)
        if (
            self.facts.existing is not None
            and self.facts.existing.requires_drop_before_replace
        ):
            runtime.drop_if_exists(relations.existing)
        runtime.create_from_query(
            relations.target,
            auto_begin=self.statement == MaterializationStatementStrategy.AUTO_BEGIN,
        )
        runtime.run_hooks("post", inside_transaction=True)
        runtime.apply_grants(
            relations.target,
            existing=relations.existing,
            full_refresh=True,
        )
        runtime.persist_docs(relations.target)
        runtime.invoke_teardown(self.teardown_macro, envelope)
        return relations.target


@dataclass(frozen=True)
class StageAndSwapView:
    """Create a view under a work name, then exchange it with the target."""

    provenance: Tuple[PlanProvenance, ...]
    facts: Optional[ViewMaterializationFacts] = None

    def resolve(self, facts: ViewMaterializationFacts) -> "StageAndSwapView":
        return replace(self, facts=facts)

    def supports_native_execution(self, runtime: MaterializationRuntime) -> bool:
        return True

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        if self.facts is None:
            return runtime.resolve_view_strategy(self).execute(runtime)

        relations = runtime.view_relations()
        runtime.run_hooks("pre", inside_transaction=False)
        runtime.drop_if_exists(relations.preexisting_intermediate)
        runtime.drop_if_exists(relations.preexisting_backup)
        runtime.run_hooks("pre", inside_transaction=True)
        runtime.create_view_from_query(relations.intermediate)

        existing = runtime.reload_relation(relations.existing)
        if existing is not None:
            assert self.facts.relation.existing is not None
            if self.facts.relation.existing.can_be_renamed:
                runtime.rename(existing, relations.backup)
            else:
                runtime.drop_if_exists(existing)
        runtime.rename(relations.intermediate, relations.target)
        runtime.apply_grants(relations.target, existing=existing, full_refresh=True)
        runtime.persist_docs(relations.target)
        runtime.run_hooks("post", inside_transaction=True)
        runtime.commit()
        runtime.drop_if_exists(relations.backup)
        runtime.run_hooks("post", inside_transaction=False)
        return relations.target


@dataclass(frozen=True)
class DirectReplaceView:
    """Replace a view through one adapter-atomic create-or-replace statement."""

    statement: MaterializationStatementStrategy
    incompatible_relation: IncompatibleRelationStrategy
    provenance: Tuple[PlanProvenance, ...]
    setup_macro: Optional[str] = None
    teardown_macro: Optional[str] = None
    persist_column_docs: bool = True
    grant_access: bool = False
    facts: Optional[ViewMaterializationFacts] = None

    def __post_init__(self) -> None:
        if (self.setup_macro is None) != (self.teardown_macro is None):
            raise ValueError("Direct-view setup and teardown macros must be paired")

    def resolve(self, facts: ViewMaterializationFacts) -> "DirectReplaceView":
        return replace(self, facts=facts)

    def supports_native_execution(self, runtime: MaterializationRuntime) -> bool:
        return True

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        if self.facts is None:
            envelope = runtime.invoke_setup(self.setup_macro)
            resolved = runtime.resolve_view_strategy(self)
            if not isinstance(resolved, DirectReplaceView):
                raise TypeError(
                    "Direct view resolution returned an incompatible strategy"
                )
            return resolved._execute_resolved(runtime, envelope)

        envelope = runtime.invoke_setup(self.setup_macro)
        return self._execute_resolved(runtime, envelope)

    def _execute_resolved(
        self, runtime: MaterializationRuntime, envelope: Any
    ) -> BaseRelation:
        assert self.facts is not None
        relations = runtime.view_relations()
        runtime.run_hooks("pre", inside_transaction=True)
        existing = relations.existing
        existing_facts = self.facts.relation.existing
        if existing is not None and existing_facts is not None:
            existing_type = existing_facts.relation.relation_type
            if existing_type != "view":
                if (
                    self.incompatible_relation == IncompatibleRelationStrategy.DROP
                    or self.facts.full_refresh
                ):
                    runtime.drop_if_exists(existing)
                else:
                    runtime.raise_wrong_relation_type(existing, "view")

        runtime.create_view_from_query(
            relations.target,
            auto_begin=self.statement == MaterializationStatementStrategy.AUTO_BEGIN,
        )
        existing_view = (
            existing
            if existing_facts is not None
            and existing_facts.relation.relation_type == "view"
            else None
        )
        runtime.apply_grants(
            relations.target, existing=existing_view, full_refresh=True
        )
        runtime.run_hooks("post", inside_transaction=True)
        runtime.persist_docs(
            relations.target,
            for_columns=self.persist_column_docs,
        )
        if self.grant_access:
            runtime.grant_view_access(relations.target)
        runtime.invoke_teardown(self.teardown_macro, envelope)
        return relations.target


@runtime_checkable
class SnapshotMaterializationStrategy(Protocol):
    provenance: Tuple[PlanProvenance, ...]

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation: ...


@dataclass(frozen=True)
class SnapshotMaterializationPlan:
    """Adapter-selected resolver for a built-in snapshot lifecycle."""

    materialization_macro_id: str
    provenance: Tuple[PlanProvenance, ...]
    setup_macro: Optional[str] = None
    teardown_macro: Optional[str] = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.materialization_macro_id, str)
            or not self.materialization_macro_id
        ):
            raise ValueError("Snapshot materialization macro id must be non-empty")
        if not isinstance(self.provenance, tuple) or not self.provenance:
            raise ValueError("Snapshot materialization requires immutable provenance")
        if not all(isinstance(item, PlanProvenance) for item in self.provenance):
            raise TypeError("Snapshot materialization provenance must be typed")
        if (self.setup_macro is None) != (self.teardown_macro is None):
            raise ValueError("Snapshot setup and teardown macros must be paired")

    def resolve(
        self, facts: SnapshotMaterializationFacts
    ) -> SnapshotMaterializationStrategy:
        provenance = self.provenance + (
            PlanProvenance(
                rule="snapshot.lifecycle.runtime_facts",
                detail=(
                    "Snapshot lifecycle resolved from target, strategy, catalog, "
                    "format, and runtime facts"
                ),
            ),
        )
        if facts.target_exists:
            return MergeExistingSnapshot(facts=facts, provenance=provenance)
        return CreateInitialSnapshot(facts=facts, provenance=provenance)

    def supports_native_execution(self, runtime: MaterializationRuntime) -> bool:
        return runtime.supports_snapshot_materialization()

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        envelope = (
            runtime.invoke_setup(self.setup_macro)
            if self.setup_macro is not None
            else None
        )
        strategy = runtime.resolve_snapshot_strategy(self)
        target = strategy.execute(runtime)
        if self.teardown_macro is not None:
            runtime.invoke_teardown(self.teardown_macro, envelope)
        return target


@dataclass(frozen=True)
class CreateInitialSnapshot:
    facts: SnapshotMaterializationFacts
    provenance: Tuple[PlanProvenance, ...]

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        relations = runtime.snapshot_relations()
        _prepare_snapshot(runtime)
        query = runtime.build_snapshot_initial_query(self.facts.strategy)
        runtime.check_snapshot_time_data_types(query)
        runtime.create_from_query(relations.target, query=query)
        _apply_snapshot_metadata(
            runtime,
            relations.target,
            existing=None,
        )
        runtime.create_indexes(relations.target)
        runtime.run_hooks("post", inside_transaction=True)
        runtime.commit()
        runtime.run_hooks("post", inside_transaction=False)
        return relations.target


@dataclass(frozen=True)
class MergeExistingSnapshot:
    facts: SnapshotMaterializationFacts
    provenance: Tuple[PlanProvenance, ...]

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        relations = runtime.snapshot_relations()
        _prepare_snapshot(runtime)
        runtime.validate_snapshot_target(relations.target, self.facts.strategy)
        staging_query = runtime.build_snapshot_staging_query(
            self.facts.strategy, relations.target
        )
        runtime.create_from_query(
            relations.staging,
            temporary=True,
            query=staging_query,
        )
        runtime.expand_target_columns(relations.staging, relations.target)
        insert_columns = runtime.reconcile_snapshot_columns(
            relations.staging,
            relations.target,
            self.facts.strategy,
        )
        runtime.check_snapshot_time_data_types(staging_query)
        runtime.execute_snapshot_merge(
            relations.target,
            relations.staging,
            insert_columns,
        )
        _apply_snapshot_metadata(
            runtime,
            relations.target,
            existing=relations.target,
        )
        runtime.run_hooks("post", inside_transaction=True)
        runtime.commit()
        runtime.post_snapshot(relations.staging)
        runtime.run_hooks("post", inside_transaction=False)
        return relations.target


@runtime_checkable
class IncrementalMaterializationStrategy(Protocol):
    provenance: Tuple[PlanProvenance, ...]

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation: ...


@dataclass(frozen=True)
class IncrementalMaterializationPlan:
    """Adapter-selected resolver for one built-in incremental materialization."""

    materialization_macro_id: str
    provenance: Tuple[PlanProvenance, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.materialization_macro_id, str)
            or not self.materialization_macro_id.strip()
        ):
            raise ValueError("Incremental materialization macro id must be non-empty")
        if not isinstance(self.provenance, tuple) or not self.provenance:
            raise ValueError(
                "Incremental materialization plan requires immutable provenance"
            )
        if not all(isinstance(item, PlanProvenance) for item in self.provenance):
            raise TypeError("Incremental materialization provenance must be typed")

    def resolve(
        self,
        mutation: IncrementalMutationPlan,
        facts: IncrementalLifecycleFacts,
    ) -> IncrementalMaterializationStrategy:
        provenance = self.provenance + (
            PlanProvenance(
                rule="incremental.lifecycle.runtime_facts",
                detail=(
                    "Incremental lifecycle resolved from mutation, schema, relation, "
                    "catalog, format, and runtime facts"
                ),
            ),
        )
        if facts.table.existing is None:
            return CreateInitial(mutation=mutation, provenance=provenance)
        if facts.full_refresh:
            return FullRefresh(mutation=mutation, provenance=provenance)
        return MutateExisting(
            mutation=mutation,
            schema_change=facts.schema_change,
            partition=facts.partition,
            provenance=provenance,
            expand_target_columns=not facts.contract_enforced,
        )

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        return runtime.resolve_incremental_strategy(self).execute(runtime)

    def supports_native_execution(self, runtime: MaterializationRuntime) -> bool:
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialization_macro_id": self.materialization_macro_id,
            "provenance": [item.to_dict() for item in self.provenance],
        }


@dataclass(frozen=True)
class CreateInitial:
    """Create the incremental target when no relation exists."""

    mutation: IncrementalMutationPlan
    provenance: Tuple[PlanProvenance, ...]
    create_indexes: bool = True
    auto_begin: bool = True

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        relations = runtime.incremental_relations(self.mutation)
        _prepare_incremental(runtime, relations)
        runtime.create_from_query(relations.target, auto_begin=self.auto_begin)
        if self.create_indexes:
            runtime.create_indexes(relations.target)
        _finalize_incremental(runtime, relations, full_refresh=False)
        return relations.target


@dataclass(frozen=True)
class FullRefresh:
    """Rebuild an existing incremental target through a stage-and-swap."""

    mutation: IncrementalMutationPlan
    provenance: Tuple[PlanProvenance, ...]
    auto_begin: bool = True

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        relations = runtime.incremental_relations(self.mutation)
        _prepare_incremental(runtime, relations)
        runtime.create_from_query(relations.intermediate, auto_begin=self.auto_begin)
        runtime.create_indexes(relations.intermediate)
        assert relations.existing is not None
        runtime.rename(relations.existing, relations.backup)
        runtime.rename(relations.intermediate, relations.target)
        _finalize_incremental(runtime, relations, full_refresh=True)
        runtime.drop_if_exists(relations.backup)
        return relations.target


@dataclass(frozen=True)
class MutateExisting:
    """Build a staging relation, reconcile schema, and mutate the target."""

    mutation: IncrementalMutationPlan
    schema_change: IncrementalSchemaChangePlan
    provenance: Tuple[PlanProvenance, ...]
    partition: Optional[IncrementalPartitionFacts] = None
    expand_target_columns: bool = True

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        relations = runtime.incremental_relations(self.mutation)
        _prepare_incremental(runtime, relations)
        runtime.create_from_query(
            relations.staging,
            temporary=self.mutation.catalog_staging.value != "permanent_table_only",
        )
        if self.expand_target_columns:
            runtime.expand_target_columns(relations.staging, relations.target)
        destination_columns = runtime.process_schema_changes(
            self.schema_change,
            relations.staging,
            relations.existing or relations.target,
        )
        runtime.execute_incremental_mutation(
            self.mutation,
            relations,
            destination_columns,
            self.partition,
        )
        _finalize_incremental(runtime, relations, full_refresh=False)
        return relations.target


@dataclass(frozen=True)
class DirectCreateInitial:
    """Create an initial target on a statement-atomic, non-transactional adapter."""

    mutation: IncrementalMutationPlan
    provenance: Tuple[PlanProvenance, ...]
    partition: Optional[IncrementalPartitionFacts] = None

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        relations = runtime.incremental_relations(self.mutation)
        runtime.run_hooks("pre", inside_transaction=True)
        runtime.create_from_query(relations.target, auto_begin=False)
        if self.partition is not None and self.partition.time_ingestion_partitioning:
            runtime.insert_from_query(relations.target, self.partition)
        _finalize_direct_incremental(runtime, relations, full_refresh=False)
        return relations.target


@dataclass(frozen=True)
class DirectFullRefresh:
    """Replace an existing target directly on a statement-atomic adapter."""

    mutation: IncrementalMutationPlan
    provenance: Tuple[PlanProvenance, ...]
    requires_drop: bool
    partition: Optional[IncrementalPartitionFacts] = None

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        relations = runtime.incremental_relations(self.mutation)
        runtime.run_hooks("pre", inside_transaction=True)
        if self.requires_drop:
            runtime.drop_if_exists(relations.existing)
        runtime.create_from_query(relations.target, auto_begin=False)
        if self.partition is not None and self.partition.time_ingestion_partitioning:
            runtime.insert_from_query(relations.target, self.partition)
        _finalize_direct_incremental(runtime, relations, full_refresh=True)
        return relations.target


@dataclass(frozen=True)
class DirectMutateExisting:
    """Mutate a target through a temporary table on a statement-atomic adapter."""

    mutation: IncrementalMutationPlan
    schema_change: IncrementalSchemaChangePlan
    provenance: Tuple[PlanProvenance, ...]
    partition: Optional[IncrementalPartitionFacts] = None
    copy_partitions: bool = False

    def execute(self, runtime: MaterializationRuntime) -> BaseRelation:
        relations = runtime.incremental_relations(self.mutation)
        runtime.run_hooks("pre", inside_transaction=True)
        runtime.create_from_query(
            relations.staging,
            temporary=self.mutation.catalog_staging.value != "permanent_table_only",
            auto_begin=False,
        )
        if self.partition is not None and self.partition.time_ingestion_partitioning:
            runtime.insert_from_query(relations.staging, self.partition)
        destination_columns = runtime.process_schema_changes(
            self.schema_change,
            relations.staging,
            relations.existing or relations.target,
        )
        if self.copy_partitions:
            assert self.partition is not None
            runtime.copy_incremental_partitions(
                relations.staging,
                relations.target,
                self.partition,
            )
        else:
            runtime.execute_incremental_mutation(
                self.mutation,
                relations,
                destination_columns,
                self.partition,
            )
        runtime.drop_if_exists(relations.staging)
        _finalize_direct_incremental(runtime, relations, full_refresh=False)
        return relations.target


def _prepare_incremental(
    runtime: MaterializationRuntime, relations: IncrementalRelationFamily
) -> None:
    runtime.drop_if_exists(relations.preexisting_intermediate)
    runtime.drop_if_exists(relations.preexisting_backup)
    runtime.run_hooks("pre", inside_transaction=False)
    runtime.run_hooks("pre", inside_transaction=True)


def _finalize_incremental(
    runtime: MaterializationRuntime,
    relations: IncrementalRelationFamily,
    *,
    full_refresh: bool,
) -> None:
    runtime.apply_grants(
        relations.target,
        existing=relations.existing,
        full_refresh=full_refresh,
    )
    runtime.persist_docs(relations.target)
    runtime.run_hooks("post", inside_transaction=True)
    runtime.commit()
    runtime.run_hooks("post", inside_transaction=False)


def _finalize_direct_incremental(
    runtime: MaterializationRuntime,
    relations: IncrementalRelationFamily,
    *,
    full_refresh: bool,
) -> None:
    runtime.run_hooks("post", inside_transaction=True)
    runtime.apply_grants(
        relations.target,
        existing=relations.existing,
        full_refresh=full_refresh,
    )
    runtime.persist_docs(relations.target)


def _prepare_snapshot(runtime: MaterializationRuntime) -> None:
    runtime.run_hooks("pre", inside_transaction=False)
    runtime.run_hooks("pre", inside_transaction=True)


def _apply_snapshot_metadata(
    runtime: MaterializationRuntime,
    target: BaseRelation,
    *,
    existing: Optional[BaseRelation],
) -> None:
    runtime.apply_grants(
        target,
        existing=existing,
        full_refresh=False,
    )
    runtime.persist_docs(target)
