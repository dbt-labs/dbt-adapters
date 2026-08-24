from unittest.mock import MagicMock

import pytest

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.planning import (
    CatalogBindingState,
    CatalogFacts,
    CreateFromQueryFacts,
    DdlAtomicity,
    ExistingRelationFacts,
    ExistingIndexStrategy,
    FormatFacts,
    IncrementalLifecyclePlan,
    IncrementalMutationFacts,
    IncrementalMutationPlan,
    IncrementalMutationStrategy,
    MaterializationExecutionFacts,
    MaterializationHookStrategy,
    MaterializationOperation,
    MaterializationOperationKind,
    MaterializationRelationRole,
    MaterializationStatementStrategy,
    MaterializationTransactionStrategy,
    MaterializationTransactionMode,
    PlanProvenance,
    RelationFacts,
    RuntimeFacts,
    TableDocumentationStrategy,
    TableIndexStrategy,
    TableLifecyclePlan,
    TableMaterializationFacts,
    TableReplacementStrategy,
    resolve_table_materialization_operations,
)


def _adapter() -> MagicMock:
    adapter = MagicMock(spec=BaseAdapter)
    adapter.plan_table_materialization = BaseAdapter.plan_table_materialization.__get__(adapter)
    return adapter


def _provenance():
    return (PlanProvenance(rule="test.lifecycle", detail="Test lifecycle selection"),)


def test_default_sql_table_resolves_to_stage_and_swap() -> None:
    plan = BaseAdapter.plan_table_materialization(
        _adapter(),
        "macro.dbt.materialization_table_default",
        "sql",
    )

    assert plan == TableLifecyclePlan.stage_and_swap(provenance=plan.provenance)
    assert plan.to_dict() == {
        "replacement": "stage_and_swap",
        "indexes": "before_swap",
        "existing_indexes": "preserve",
        "documentation": "before_commit",
        "transaction": "explicit_commit",
        "hooks": "split",
        "statement": "auto_begin",
        "setup_macro": None,
        "teardown_macro": None,
        "facts": None,
        "operations": [],
        "provenance": [
            {
                "rule": "materialization.table.default",
                "detail": ("Built-in SQL table materialization uses stage-and-swap replacement"),
            }
        ],
    }


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
    assert BaseAdapter.plan_table_materialization(_adapter(), macro_id, language) is None


def test_direct_replace_supports_a_paired_execution_envelope() -> None:
    plan = TableLifecyclePlan.direct_replace(
        setup_macro="set_query_tag",
        teardown_macro="unset_query_tag",
        provenance=_provenance(),
    )

    assert plan.replacement == TableReplacementStrategy.DIRECT_REPLACE
    assert plan.indexes == TableIndexStrategy.NONE
    assert plan.transaction == MaterializationTransactionStrategy.ADAPTER_MANAGED
    assert plan.hooks == MaterializationHookStrategy.IN_TRANSACTION


def test_post_commit_documentation_requires_explicit_transaction_control() -> None:
    with pytest.raises(ValueError, match="Post-commit documentation"):
        TableLifecyclePlan(
            replacement=TableReplacementStrategy.STAGE_AND_SWAP,
            indexes=TableIndexStrategy.AFTER_SWAP,
            existing_indexes=ExistingIndexStrategy.DROP_BEFORE_SWAP,
            documentation=TableDocumentationStrategy.AFTER_COMMIT,
            transaction=MaterializationTransactionStrategy.ADAPTER_MANAGED,
            hooks=MaterializationHookStrategy.SPLIT,
            statement=MaterializationStatementStrategy.AUTO_BEGIN,
            provenance=_provenance(),
        )


def test_execution_envelope_macros_must_be_paired() -> None:
    with pytest.raises(ValueError, match="must be paired"):
        TableLifecyclePlan.direct_replace(
            setup_macro="set_query_tag",
            provenance=_provenance(),
        )


def test_materialization_operation_serializes_renderer_variant() -> None:
    operation = MaterializationOperation(
        kind=MaterializationOperationKind.OPTIMIZE,
        relation=MaterializationRelationRole.TARGET,
        renderer_variant="zorder",
    )

    assert operation.to_dict()["renderer_variant"] == "zorder"


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


def test_stage_and_swap_program_drops_unrenameable_existing_relation() -> None:
    plan = TableLifecyclePlan.stage_and_swap(provenance=_provenance())
    facts = _facts(can_be_renamed=False)

    resolved = plan.resolve(
        facts=facts,
        operations=resolve_table_materialization_operations(plan, facts),
    )

    existing_mutations = [
        operation.kind
        for operation in resolved.operations
        if operation.relation is not None and operation.relation.value == "existing"
    ]
    assert existing_mutations == [MaterializationOperationKind.DROP_RELATION_IF_EXISTS]
    assert resolved.is_resolved
    assert resolved.to_dict()["facts"]["existing"]["can_be_renamed"] is False


def test_direct_replace_program_carries_drop_decision_and_envelope() -> None:
    plan = TableLifecyclePlan.direct_replace(
        setup_macro="set_query_tag",
        teardown_macro="unset_query_tag",
        provenance=_provenance(),
    )
    facts = _facts(requires_drop=True)

    operations = resolve_table_materialization_operations(plan, facts)
    resolved = plan.resolve(facts=facts, operations=operations)

    assert [operation.kind for operation in resolved.operations] == [
        MaterializationOperationKind.INVOKE_CALLBACK,
        MaterializationOperationKind.RUN_HOOKS,
        MaterializationOperationKind.DROP_RELATION_IF_EXISTS,
        MaterializationOperationKind.CREATE_FROM_QUERY,
        MaterializationOperationKind.RUN_HOOKS,
        MaterializationOperationKind.APPLY_GRANTS,
        MaterializationOperationKind.PERSIST_DOCUMENTATION,
        MaterializationOperationKind.INVOKE_CALLBACK,
    ]


def test_base_adapter_builds_live_replacement_facts() -> None:
    adapter = MagicMock()
    create_facts = _facts().create
    adapter.build_create_from_query_facts.return_value = create_facts
    adapter.get_table_materialization_execution_facts.return_value = MaterializationExecutionFacts(
        transaction_mode=MaterializationTransactionMode.TRANSACTIONAL
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


def test_incremental_lifecycle_program_carries_schema_and_mutation_order() -> None:
    adapter = MagicMock()
    adapter.build_table_materialization_facts.return_value = _facts()
    adapter.plan_incremental_schema_change = BaseAdapter.plan_incremental_schema_change.__get__(
        adapter
    )
    mutation = IncrementalMutationPlan(
        requested_strategy="merge",
        strategy=IncrementalMutationStrategy.MERGE,
        renderer_macro="get_incremental_merge_sql",
        atomicity=DdlAtomicity.TRANSACTION,
        provenance=(
            PlanProvenance(
                rule="test.incremental.merge",
                detail="Test merge strategy",
            ),
        ),
        facts=IncrementalMutationFacts(
            requested_strategy="merge",
            language="sql",
            unique_key_present=False,
        ),
    )

    plan = BaseAdapter.resolve_incremental_lifecycle_plan(
        adapter,
        mutation,
        MagicMock(),
        MagicMock(),
        MagicMock(),
        full_refresh=False,
        on_schema_change="sync_all_columns",
        staging_is_temporary=False,
        contract_enforced=False,
    )

    assert isinstance(plan, IncrementalLifecyclePlan)
    assert plan.schema_change.strategy.value == "sync_all_columns"
    assert [operation.kind.value for operation in plan.operations[4:8]] == [
        "create_from_query",
        "expand_target_column_types",
        "process_schema_changes",
        "execute_incremental_mutation",
    ]
    assert plan.operations[4].relation.value == "temp"
    assert plan.operations[4].temporary is False
