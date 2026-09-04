from dataclasses import replace
from unittest.mock import MagicMock, call

import pytest
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.planning import (
    CatalogBindingState,
    CatalogFacts,
    CreateFromQueryFacts,
    DdlAtomicity,
    DirectMutateExisting,
    DirectReplaceTable,
    DirectReplaceView,
    ExistingIndexStrategy,
    ExistingRelationFacts,
    FormatFacts,
    IncompatibleRelationStrategy,
    IncrementalCatalogStaging,
    IncrementalMaterializationPlan,
    IncrementalMutationFacts,
    IncrementalMutationPlan,
    IncrementalMutationStrategy,
    IncrementalRelationFamily,
    IncrementalSchemaChangePlan,
    IncrementalSchemaChangeStrategy,
    MaterializationExecutionFacts,
    MaterializationHookStrategy,
    MaterializationStatementStrategy,
    MaterializationTransactionMode,
    MaterializationTransactionStrategy,
    MutateExisting,
    PlanProvenance,
    RelationFacts,
    RuntimeFacts,
    SnapshotHardDeletes,
    SnapshotMaterializationFacts,
    SnapshotMaterializationPlan,
    SnapshotRelationFamily,
    SnapshotStrategyFacts,
    StageAndSwapTable,
    StageAndSwapView,
    TableDocumentationStrategy,
    TableIndexStrategy,
    TableMaterializationFacts,
    TableRelationFamily,
    ViewMaterializationFacts,
)


def _adapter() -> MagicMock:
    adapter = MagicMock(spec=BaseAdapter)
    adapter.plan_table_materialization = BaseAdapter.plan_table_materialization.__get__(
        adapter
    )
    adapter.plan_view_materialization = BaseAdapter.plan_view_materialization.__get__(
        adapter
    )
    adapter.plan_snapshot_materialization = (
        BaseAdapter.plan_snapshot_materialization.__get__(adapter)
    )
    return adapter


def _provenance():
    return (PlanProvenance(rule="test.lifecycle", detail="Test lifecycle selection"),)


def _facts(*, can_be_renamed: bool = True, requires_drop: bool = False):
    relation = RelationFacts(
        database="db",
        schema="schema",
        identifier="table",
        relation_type="table",
    )
    return TableMaterializationFacts(
        create=CreateFromQueryFacts(
            relation=relation,
            catalog=CatalogFacts(state=CatalogBindingState.UNBOUND),
            format=FormatFacts(),
            runtime=RuntimeFacts(engine="test"),
        ),
        existing=ExistingRelationFacts(
            relation=relation,
            format=FormatFacts(),
            can_be_renamed=can_be_renamed,
            can_be_replaced=False,
            requires_drop_before_replace=requires_drop,
        ),
    )


def _snapshot_facts(*, target_exists: bool) -> SnapshotMaterializationFacts:
    table = _facts()
    if not target_exists:
        table = replace(table, existing=None)
    return SnapshotMaterializationFacts(
        table=table,
        target_exists=target_exists,
        strategy=SnapshotStrategyFacts(
            unique_key=("account_id", "line_id"),
            updated_at="source_data.updated_at",
            row_changed="snapshotted_data.updated_at < source_data.updated_at",
            scd_id="md5(source_data.account_id)",
            hard_deletes=SnapshotHardDeletes.INVALIDATE,
        ),
    )


def test_default_sql_table_resolves_to_stage_and_swap() -> None:
    plan = BaseAdapter.plan_table_materialization(
        _adapter(),
        "macro.dbt.materialization_table_default",
        "sql",
    )

    assert isinstance(plan, StageAndSwapTable)
    assert plan.indexes == TableIndexStrategy.BEFORE_SWAP
    assert plan.existing_indexes == ExistingIndexStrategy.PRESERVE
    assert plan.documentation == TableDocumentationStrategy.BEFORE_COMMIT
    assert plan.transaction == MaterializationTransactionStrategy.EXPLICIT_COMMIT
    assert plan.hooks == MaterializationHookStrategy.SPLIT
    assert plan.statement == MaterializationStatementStrategy.AUTO_BEGIN
    assert plan.facts is None


@pytest.mark.parametrize(
    "macro_id,language",
    [
        ("macro.project.materialization_table_default", "sql"),
        ("macro.dbt.materialization_table_default", "python"),
    ],
)
def test_default_resolver_leaves_overrides_and_non_sql_on_jinja_path(
    macro_id: str, language: str
) -> None:
    assert (
        BaseAdapter.plan_table_materialization(_adapter(), macro_id, language) is None
    )


def test_semantic_strategies_validate_their_execution_envelopes() -> None:
    with pytest.raises(ValueError, match="must be paired"):
        DirectReplaceTable(
            statement=MaterializationStatementStrategy.NO_AUTO_BEGIN,
            setup_macro="set_query_tag",
            provenance=_provenance(),
        )

    with pytest.raises(ValueError, match="Post-commit documentation"):
        StageAndSwapTable(
            indexes=TableIndexStrategy.AFTER_SWAP,
            existing_indexes=ExistingIndexStrategy.DROP_BEFORE_SWAP,
            documentation=TableDocumentationStrategy.AFTER_COMMIT,
            transaction=MaterializationTransactionStrategy.ADAPTER_MANAGED,
            hooks=MaterializationHookStrategy.SPLIT,
            statement=MaterializationStatementStrategy.AUTO_BEGIN,
            provenance=_provenance(),
        )


def test_base_adapter_builds_live_replacement_facts() -> None:
    adapter = MagicMock()
    create_facts = _facts().create
    adapter.build_create_from_query_facts.return_value = create_facts
    adapter.get_table_materialization_execution_facts.return_value = (
        MaterializationExecutionFacts(
            transaction_mode=MaterializationTransactionMode.TRANSACTIONAL
        )
    )
    adapter._create_from_query_fact_value.side_effect = lambda value, **_: (
        None if value is None else str(getattr(value, "value", value)).casefold()
    )
    target = MagicMock()
    target.needs_to_drop.return_value = True
    existing = MagicMock()
    existing.database = "DB"
    existing.schema = "SCHEMA"
    existing.identifier = "TABLE"
    existing.type = "view"
    existing.table_format = "iceberg"
    existing.file_format = "parquet"
    existing.can_be_renamed = False
    existing.can_be_replaced = False
    existing.is_shallow_clone = False

    facts = BaseAdapter.build_table_materialization_facts(
        adapter,
        MagicMock(),
        target,
        existing,
    )

    target.needs_to_drop.assert_called_once_with(existing)
    assert facts.existing is not None
    assert facts.existing.relation.relation_type == "view"
    assert facts.existing.format.table_format == "iceberg"
    assert facts.existing.requires_drop_before_replace is True


def test_incremental_resolution_returns_a_semantic_mutation_strategy() -> None:
    adapter = MagicMock()
    adapter.build_table_materialization_facts.return_value = _facts()
    adapter.plan_incremental_schema_change = (
        BaseAdapter.plan_incremental_schema_change.__get__(adapter)
    )
    mutation = IncrementalMutationPlan(
        requested_strategy="merge",
        strategy=IncrementalMutationStrategy.MERGE,
        renderer_macro="get_incremental_merge_sql",
        atomicity=DdlAtomicity.TRANSACTION,
        provenance=_provenance(),
        facts=IncrementalMutationFacts(
            requested_strategy="merge",
            language="sql",
            unique_key_present=False,
        ),
    )

    facts = BaseAdapter.build_incremental_lifecycle_facts(
        adapter,
        mutation,
        MagicMock(),
        MagicMock(),
        MagicMock(),
        full_refresh=False,
        on_schema_change="sync_all_columns",
        contract_enforced=False,
    )
    materialization = IncrementalMaterializationPlan(
        materialization_macro_id="macro.dbt.materialization_incremental_default",
        provenance=_provenance(),
    )
    strategy = materialization.resolve(mutation, facts)

    assert isinstance(strategy, MutateExisting)
    assert strategy.mutation is mutation
    assert strategy.schema_change.strategy.value == "sync_all_columns"


def test_stage_and_swap_strategy_exposes_readable_database_control_flow() -> None:
    target = MagicMock(name="target")
    existing = MagicMock(name="existing")
    intermediate = MagicMock(name="intermediate")
    backup = MagicMock(name="backup")
    old_intermediate = MagicMock(name="old_intermediate")
    old_backup = MagicMock(name="old_backup")
    runtime = MagicMock()
    runtime.table_relations.return_value = TableRelationFamily(
        target=target,
        existing=existing,
        intermediate=intermediate,
        backup=backup,
        preexisting_intermediate=old_intermediate,
        preexisting_backup=old_backup,
    )
    runtime.reload_relation.return_value = existing
    strategy = StageAndSwapTable(
        indexes=TableIndexStrategy.BEFORE_SWAP,
        existing_indexes=ExistingIndexStrategy.PRESERVE,
        documentation=TableDocumentationStrategy.BEFORE_COMMIT,
        transaction=MaterializationTransactionStrategy.EXPLICIT_COMMIT,
        hooks=MaterializationHookStrategy.SPLIT,
        statement=MaterializationStatementStrategy.AUTO_BEGIN,
        provenance=_provenance(),
    ).resolve(_facts(can_be_renamed=False))

    assert strategy.execute(runtime) is target
    assert runtime.mock_calls == [
        call.table_relations(),
        call.drop_if_exists(old_intermediate),
        call.drop_if_exists(old_backup),
        call.run_hooks("pre", inside_transaction=False),
        call.run_hooks("pre", inside_transaction=True),
        call.create_from_query(intermediate, auto_begin=True),
        call.create_indexes(intermediate),
        call.reload_relation(existing),
        call.drop_if_exists(existing),
        call.rename(intermediate, target),
        call.run_hooks("post", inside_transaction=True),
        call.apply_grants(target, existing=existing, full_refresh=True),
        call.persist_docs(target),
        call.commit(),
        call.drop_if_exists(backup),
        call.run_hooks("post", inside_transaction=False),
    ]
    runtime.rename.assert_called_once_with(intermediate, target)


def test_direct_incremental_strategy_honors_catalog_staging_facts() -> None:
    mutation = IncrementalMutationPlan(
        requested_strategy="merge",
        strategy=IncrementalMutationStrategy.MERGE,
        renderer_macro="get_incremental_merge_sql",
        atomicity=DdlAtomicity.STATEMENT,
        provenance=_provenance(),
        facts=IncrementalMutationFacts(
            requested_strategy="merge",
            language="sql",
            unique_key_present=True,
        ),
        catalog_staging=IncrementalCatalogStaging.PERMANENT_TABLE_ONLY,
    )
    schema_change = IncrementalSchemaChangePlan(
        requested_strategy="ignore",
        strategy=IncrementalSchemaChangeStrategy.IGNORE,
        provenance=_provenance(),
    )
    target = MagicMock(name="target")
    existing = MagicMock(name="existing")
    staging = MagicMock(name="staging")
    runtime = MagicMock()
    runtime.incremental_relations.return_value = IncrementalRelationFamily(
        target=target,
        existing=existing,
        intermediate=MagicMock(name="intermediate"),
        backup=MagicMock(name="backup"),
        preexisting_intermediate=None,
        preexisting_backup=None,
        staging=staging,
    )
    runtime.process_schema_changes.return_value = (MagicMock(name="column"),)
    strategy = DirectMutateExisting(
        mutation=mutation,
        schema_change=schema_change,
        provenance=_provenance(),
    )

    assert strategy.execute(runtime) is target
    runtime.create_from_query.assert_called_once_with(
        staging,
        temporary=False,
        auto_begin=False,
    )
    runtime.execute_incremental_mutation.assert_called_once()


def test_default_view_and_snapshot_resolve_to_typed_lifecycles() -> None:
    adapter = _adapter()

    view = BaseAdapter.plan_view_materialization(
        adapter,
        "macro.dbt.materialization_view_default",
        "sql",
    )
    snapshot = BaseAdapter.plan_snapshot_materialization(
        adapter,
        "macro.dbt.materialization_snapshot_default",
        "sql",
    )

    assert isinstance(view, StageAndSwapView)
    assert isinstance(snapshot, SnapshotMaterializationPlan)


@pytest.mark.parametrize(
    "macro_id",
    [
        "macro.project.materialization_view_default",
        "macro.project.materialization_snapshot_default",
        "macro.dbt_spark.materialization_snapshot_spark",
    ],
)
def test_view_and_snapshot_overrides_remain_on_jinja_path(macro_id: str) -> None:
    adapter = _adapter()

    assert BaseAdapter.plan_view_materialization(adapter, macro_id, "sql") is None
    assert BaseAdapter.plan_snapshot_materialization(adapter, macro_id, "sql") is None


def test_direct_view_rejects_wrong_type_without_bigquery_full_refresh() -> None:
    target = MagicMock(name="target")
    existing = MagicMock(name="existing")
    runtime = MagicMock()
    runtime.view_relations.return_value = TableRelationFamily(
        target=target,
        existing=existing,
        intermediate=MagicMock(name="intermediate"),
        backup=MagicMock(name="backup"),
        preexisting_intermediate=None,
        preexisting_backup=None,
    )
    strategy = DirectReplaceView(
        statement=MaterializationStatementStrategy.NO_AUTO_BEGIN,
        incompatible_relation=IncompatibleRelationStrategy.DROP_ON_FULL_REFRESH,
        provenance=_provenance(),
    ).resolve(ViewMaterializationFacts(relation=_facts(), full_refresh=False))

    assert strategy.execute(runtime) is target
    runtime.raise_wrong_relation_type.assert_called_once_with(existing, "view")
    runtime.drop_if_exists.assert_not_called()
    runtime.create_view_from_query.assert_called_once_with(target, auto_begin=False)


def test_snapshot_plan_selects_initial_or_merge_from_typed_facts() -> None:
    plan = SnapshotMaterializationPlan(
        materialization_macro_id="macro.dbt.materialization_snapshot_default",
        provenance=_provenance(),
    )

    assert plan.resolve(_snapshot_facts(target_exists=False)).__class__.__name__ == (
        "CreateInitialSnapshot"
    )
    assert plan.resolve(_snapshot_facts(target_exists=True)).__class__.__name__ == (
        "MergeExistingSnapshot"
    )


def test_existing_snapshot_exposes_staging_schema_and_merge_control_flow() -> None:
    facts = _snapshot_facts(target_exists=True)
    target = MagicMock(name="target")
    staging = MagicMock(name="staging")
    runtime = MagicMock()
    runtime.snapshot_relations.return_value = SnapshotRelationFamily(
        target=target,
        staging=staging,
        target_exists=True,
    )
    runtime.build_snapshot_staging_query.return_value = "select staged"
    runtime.reconcile_snapshot_columns.return_value = ('"id"', '"updated_at"')
    strategy = SnapshotMaterializationPlan(
        materialization_macro_id="macro.dbt.materialization_snapshot_default",
        provenance=_provenance(),
    ).resolve(facts)

    assert strategy.execute(runtime) is target
    assert runtime.mock_calls == [
        call.snapshot_relations(),
        call.run_hooks("pre", inside_transaction=False),
        call.run_hooks("pre", inside_transaction=True),
        call.validate_snapshot_target(target, facts.strategy),
        call.build_snapshot_staging_query(facts.strategy, target),
        call.create_from_query(staging, temporary=True, query="select staged"),
        call.expand_target_columns(staging, target),
        call.reconcile_snapshot_columns(staging, target, facts.strategy),
        call.check_snapshot_time_data_types("select staged"),
        call.execute_snapshot_merge(target, staging, ('"id"', '"updated_at"')),
        call.apply_grants(target, existing=target, full_refresh=False),
        call.persist_docs(target),
        call.run_hooks("post", inside_transaction=True),
        call.commit(),
        call.post_snapshot(staging),
        call.run_hooks("post", inside_transaction=False),
    ]
