from types import SimpleNamespace

import pytest

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.planning import (
    DdlAtomicity,
    IncrementalCatalogStaging,
    IncrementalMutationArguments,
    IncrementalMutationFacts,
    IncrementalMutationStrategyOffer,
    IncrementalMutationStrategy,
    IncrementalSourceConsistency,
    IncrementalStrategyRequirements,
    IncrementalTempRelationType,
    IncrementalUniqueKeyRequirement,
    IncrementalSchemaChangeStrategy,
    PlanProvenance,
    resolve_incremental_mutation_offers,
    resolve_incremental_mutation_plan,
    resolve_incremental_schema_change_plan,
)
from dbt_common.exceptions import DbtRuntimeError

BUILTIN_STRATEGIES = [
    "append",
    "delete+insert",
    "merge",
    "insert_overwrite",
    "microbatch",
]


@pytest.mark.parametrize(
    "requested,expected_strategy,expected_macro",
    [
        (
            None,
            IncrementalMutationStrategy.ADAPTER_DEFAULT,
            "get_incremental_default_sql",
        ),
        ("append", IncrementalMutationStrategy.APPEND, "get_incremental_append_sql"),
        (
            "delete+insert",
            IncrementalMutationStrategy.DELETE_INSERT,
            "get_incremental_delete_insert_sql",
        ),
        ("merge", IncrementalMutationStrategy.MERGE, "get_incremental_merge_sql"),
        (
            "insert_overwrite",
            IncrementalMutationStrategy.INSERT_OVERWRITE,
            "get_incremental_insert_overwrite_sql",
        ),
        (
            "microbatch",
            IncrementalMutationStrategy.MICROBATCH,
            "get_incremental_microbatch_sql",
        ),
        (
            "my_strategy",
            IncrementalMutationStrategy.CUSTOM,
            "get_incremental_my_strategy_sql",
        ),
    ],
)
def test_resolves_incremental_strategy_to_renderer(requested, expected_strategy, expected_macro):
    plan = resolve_incremental_mutation_plan(
        requested,
        valid_strategies=BUILTIN_STRATEGIES,
        builtin_strategies=BUILTIN_STRATEGIES,
    )

    assert plan.strategy == expected_strategy
    assert plan.renderer_macro == expected_macro
    assert plan.atomicity == DdlAtomicity.UNKNOWN
    assert plan.reason is None


def test_unsupported_builtin_strategy_is_an_explicit_plan():
    plan = resolve_incremental_mutation_plan(
        "merge",
        valid_strategies=["append"],
        builtin_strategies=BUILTIN_STRATEGIES,
    )

    assert plan.to_dict() == {
        "requested_strategy": "merge",
        "strategy": "unsupported",
        "renderer_macro": None,
        "atomicity": "none",
        "provenance": [
            {
                "rule": "incremental.requested_strategy.unsupported",
                "detail": "The incremental strategy 'merge' is not valid for this adapter",
            }
        ],
        "requirements": {
            "unique_key": "optional",
            "source_consistency": "single_evaluation",
            "allowed_temp_relation_types": [],
            "default_temp_relation_type": None,
            "supported_languages": [],
        },
        "temp_relation_type": None,
        "catalog_staging": "standard",
        "reason": "The incremental strategy 'merge' is not valid for this adapter",
    }


def test_base_adapter_resolver_uses_adapter_strategy_support():
    adapter = SimpleNamespace(
        valid_incremental_strategies=lambda: ["append"],
        builtin_incremental_strategies=lambda: BUILTIN_STRATEGIES,
        build_incremental_mutation_facts=lambda **kwargs: BaseAdapter.build_incremental_mutation_facts(
            adapter, **kwargs
        ),
        get_incremental_catalog_staging=lambda catalog_relation: IncrementalCatalogStaging.STANDARD,
        get_incremental_mutation_strategy_offers=lambda facts: BaseAdapter.get_incremental_mutation_strategy_offers(
            adapter, facts
        ),
    )

    plan = BaseAdapter.plan_incremental_mutation(adapter, "merge")

    assert plan.strategy == IncrementalMutationStrategy.UNSUPPORTED


def test_base_adapter_resolver_passes_actual_mutation_facts_to_offers():
    captured_facts = []
    requirements = IncrementalStrategyRequirements(
        unique_key=IncrementalUniqueKeyRequirement.REQUIRED,
        source_consistency=IncrementalSourceConsistency.STABLE_REUSE,
        allowed_temp_relation_types=(IncrementalTempRelationType.TABLE,),
        default_temp_relation_type=IncrementalTempRelationType.TABLE,
        supported_languages=("python",),
    )

    def offers(facts):
        captured_facts.append(facts)
        return (
            IncrementalMutationStrategyOffer.available(
                strategy=IncrementalMutationStrategy.MERGE,
                renderer_macro="get_incremental_merge_sql",
                atomicity=DdlAtomicity.UNKNOWN,
                requirements=requirements,
                provenance=(
                    PlanProvenance(rule="test.actual_facts", detail="Actual facts accepted"),
                ),
            ),
        )

    adapter = SimpleNamespace(
        build_incremental_mutation_facts=lambda **kwargs: BaseAdapter.build_incremental_mutation_facts(
            adapter, **kwargs
        ),
        get_incremental_catalog_staging=lambda catalog_relation: IncrementalCatalogStaging.PERMANENT_TABLE_ONLY,
        get_incremental_mutation_strategy_offers=offers,
    )
    catalog_relation = object()

    plan = BaseAdapter.plan_incremental_mutation(
        adapter,
        "merge",
        language="python",
        unique_key=["account_id"],
        requested_temp_relation_type="table",
        catalog_relation=catalog_relation,
    )

    assert captured_facts == [
        IncrementalMutationFacts(
            requested_strategy="merge",
            language="python",
            unique_key_present=True,
            requested_temp_relation_type="table",
            catalog_staging=IncrementalCatalogStaging.PERMANENT_TABLE_ONLY,
        )
    ]
    assert plan.temp_relation_type == IncrementalTempRelationType.TABLE
    assert plan.catalog_staging == IncrementalCatalogStaging.PERMANENT_TABLE_ONLY


def test_plan_macro_selection_preserves_missing_macro_error():
    adapter = SimpleNamespace(
        config=SimpleNamespace(project_name="test_project"),
        get_incremental_strategy_macro=lambda context, strategy: BaseAdapter.get_incremental_strategy_macro(
            adapter, context, strategy
        ),
        plan_incremental_mutation=lambda strategy: resolve_incremental_mutation_plan(
            strategy,
            valid_strategies=["append"],
            builtin_strategies=BUILTIN_STRATEGIES,
        ),
        _get_incremental_plan_macro=lambda context, plan: BaseAdapter._get_incremental_plan_macro(
            adapter, context, plan
        ),
    )
    plan = resolve_incremental_mutation_plan(
        "my_strategy",
        valid_strategies=["append"],
        builtin_strategies=BUILTIN_STRATEGIES,
    )

    with pytest.raises(DbtRuntimeError, match="get_incremental_my_strategy_sql"):
        BaseAdapter.get_incremental_plan_macro(adapter, {}, plan)


def test_plan_macro_selection_rejects_unsupported_plan():
    adapter = SimpleNamespace(config=SimpleNamespace(project_name="test_project"))
    adapter.get_incremental_strategy_macro = BaseAdapter.get_incremental_strategy_macro.__get__(
        adapter
    )
    adapter._get_incremental_plan_macro = BaseAdapter._get_incremental_plan_macro.__get__(adapter)
    adapter.plan_incremental_mutation = lambda strategy: resolve_incremental_mutation_plan(
        strategy,
        valid_strategies=["append"],
        builtin_strategies=BUILTIN_STRATEGIES,
    )
    plan = resolve_incremental_mutation_plan(
        "merge",
        valid_strategies=["append"],
        builtin_strategies=BUILTIN_STRATEGIES,
    )

    with pytest.raises(DbtRuntimeError, match="not valid for this adapter"):
        BaseAdapter.get_incremental_plan_macro(adapter, {}, plan)


def test_plan_macro_selection_preserves_legacy_adapter_override():
    selected_macro = object()
    adapter = SimpleNamespace(
        get_incremental_strategy_macro=lambda context, strategy: selected_macro,
    )
    plan = resolve_incremental_mutation_plan(
        "my_strategy",
        valid_strategies=["append"],
        builtin_strategies=BUILTIN_STRATEGIES,
    )

    assert BaseAdapter.get_incremental_plan_macro(adapter, {}, plan) is selected_macro


def test_plan_macro_selection_delegates_rejected_plan_to_legacy_adapter_override():
    selected_macro = object()
    adapter = SimpleNamespace(
        get_incremental_strategy_macro=lambda context, strategy: selected_macro,
    )
    plan = resolve_incremental_mutation_plan(
        "merge",
        valid_strategies=["append"],
        builtin_strategies=BUILTIN_STRATEGIES,
    )

    assert plan.strategy == IncrementalMutationStrategy.UNSUPPORTED
    assert BaseAdapter.get_incremental_plan_macro(adapter, {}, plan) is selected_macro


@pytest.mark.parametrize(
    "requested,expected",
    [
        (None, IncrementalSchemaChangeStrategy.IGNORE),
        ("ignore", IncrementalSchemaChangeStrategy.IGNORE),
        ("append_new_columns", IncrementalSchemaChangeStrategy.APPEND_NEW_COLUMNS),
        ("sync_all_columns", IncrementalSchemaChangeStrategy.SYNC_ALL_COLUMNS),
        ("fail", IncrementalSchemaChangeStrategy.FAIL),
    ],
)
def test_resolves_incremental_schema_change_strategy(requested, expected):
    plan = resolve_incremental_schema_change_plan(requested)

    assert plan.strategy == expected
    assert plan.was_coerced is False


def test_invalid_incremental_schema_change_strategy_uses_default_with_provenance():
    plan = resolve_incremental_schema_change_plan("replace_everything")

    assert plan.to_dict() == {
        "requested_strategy": "replace_everything",
        "strategy": "ignore",
        "provenance": [
            {
                "rule": "incremental.schema_change.invalid_default",
                "detail": (
                    "Invalid value for on_schema_change (replace_everything) specified. "
                    "Setting default value of ignore."
                ),
            }
        ],
    }
    assert plan.was_coerced is True


def test_incremental_schema_change_resolver_honors_compatibility_macro_default():
    plan = resolve_incremental_schema_change_plan("replace_everything", default="fail")

    assert plan.strategy == IncrementalSchemaChangeStrategy.FAIL
    assert plan.was_coerced is True


def test_incremental_arguments_normalize_at_the_legacy_macro_boundary():
    target_relation = object()
    temp_relation = object()
    dest_columns = [object(), object()]

    arguments = IncrementalMutationArguments.from_values(
        target_relation=target_relation,
        temp_relation=temp_relation,
        unique_key=["account_id", "event_id"],
        dest_columns=dest_columns,
        incremental_predicates=["DBT_INTERNAL_DEST.event_at > date '2026-01-01'"],
        adapter_arguments={"catalog_relation": "catalog"},
    )

    assert arguments.unique_key == ("account_id", "event_id")
    assert arguments.dest_columns == tuple(dest_columns)
    assert arguments.incremental_predicates == ("DBT_INTERNAL_DEST.event_at > date '2026-01-01'",)
    assert arguments.to_macro_dict() == {
        "target_relation": target_relation,
        "temp_relation": temp_relation,
        "unique_key": ["account_id", "event_id"],
        "dest_columns": dest_columns,
        "incremental_predicates": ["DBT_INTERNAL_DEST.event_at > date '2026-01-01'"],
        "catalog_relation": "catalog",
    }


def test_incremental_offer_resolves_typed_staging_requirements():
    facts = IncrementalMutationFacts(
        requested_strategy="delete+insert",
        language="sql",
        unique_key_present=True,
        requested_temp_relation_type="transient",
    )
    requirements = IncrementalStrategyRequirements(
        unique_key=IncrementalUniqueKeyRequirement.OPTIONAL,
        source_consistency=IncrementalSourceConsistency.STABLE_REUSE,
        allowed_temp_relation_types=(
            IncrementalTempRelationType.TABLE,
            IncrementalTempRelationType.TRANSIENT,
        ),
        default_temp_relation_type=IncrementalTempRelationType.TABLE,
    )
    offer = IncrementalMutationStrategyOffer.available(
        strategy=IncrementalMutationStrategy.DELETE_INSERT,
        renderer_macro="get_incremental_delete_insert_sql",
        atomicity=DdlAtomicity.UNKNOWN,
        requirements=requirements,
        provenance=(PlanProvenance(rule="test.offer", detail="test offer"),),
    )

    plan = resolve_incremental_mutation_offers(facts=facts, offers=(offer,))

    assert plan.strategy == IncrementalMutationStrategy.DELETE_INSERT
    assert plan.requirements == requirements
    assert plan.temp_relation_type == IncrementalTempRelationType.TRANSIENT


def test_incremental_offer_preserves_rejections_before_selected_fallback():
    facts = IncrementalMutationFacts(
        requested_strategy="merge",
        language="sql",
        unique_key_present=True,
    )
    requirements = IncrementalStrategyRequirements(
        unique_key=IncrementalUniqueKeyRequirement.OPTIONAL,
        source_consistency=IncrementalSourceConsistency.SINGLE_EVALUATION,
    )
    rejected = IncrementalMutationStrategyOffer.rejected(
        strategy=IncrementalMutationStrategy.MERGE,
        reason="Preferred merge implementation is unavailable",
        requirements=requirements,
        provenance=(
            PlanProvenance(
                rule="test.preferred.rejected",
                detail="Preferred merge implementation is unavailable",
            ),
        ),
    )
    fallback = IncrementalMutationStrategyOffer.available(
        strategy=IncrementalMutationStrategy.MERGE,
        renderer_macro="get_incremental_merge_sql",
        atomicity=DdlAtomicity.UNKNOWN,
        requirements=requirements,
        provenance=(PlanProvenance(rule="test.fallback", detail="Fallback selected"),),
    )

    plan = resolve_incremental_mutation_offers(facts=facts, offers=(rejected, fallback))

    assert plan.provenance == rejected.provenance + fallback.provenance


@pytest.mark.parametrize("unique_key", [None, "", "   ", [], ()])
def test_incremental_facts_treat_empty_unique_keys_as_absent(unique_key):
    adapter = SimpleNamespace(
        get_incremental_catalog_staging=lambda catalog_relation: IncrementalCatalogStaging.STANDARD
    )

    facts = BaseAdapter.build_incremental_mutation_facts(
        adapter,
        requested_strategy="merge",
        language="sql",
        unique_key=unique_key,
        requested_temp_relation_type=None,
        catalog_relation=None,
    )

    assert facts.unique_key_present is False


def test_incremental_facts_reject_invalid_unique_key_columns():
    adapter = SimpleNamespace(
        get_incremental_catalog_staging=lambda catalog_relation: IncrementalCatalogStaging.STANDARD
    )

    with pytest.raises(ValueError, match="non-empty strings"):
        BaseAdapter.build_incremental_mutation_facts(
            adapter,
            requested_strategy="merge",
            language="sql",
            unique_key=["account_id", ""],
            requested_temp_relation_type=None,
            catalog_relation=None,
        )


def test_incremental_plan_carries_resolved_catalog_staging_to_renderer():
    facts = IncrementalMutationFacts(
        requested_strategy="merge",
        language="sql",
        unique_key_present=True,
        catalog_staging=IncrementalCatalogStaging.PERMANENT_TABLE_ONLY,
    )
    requirements = IncrementalStrategyRequirements(
        unique_key=IncrementalUniqueKeyRequirement.OPTIONAL,
        source_consistency=IncrementalSourceConsistency.SINGLE_EVALUATION,
        allowed_temp_relation_types=(IncrementalTempRelationType.TABLE,),
        default_temp_relation_type=IncrementalTempRelationType.TABLE,
    )
    offer = IncrementalMutationStrategyOffer.available(
        strategy=IncrementalMutationStrategy.MERGE,
        renderer_macro="get_incremental_merge_sql",
        atomicity=DdlAtomicity.UNKNOWN,
        requirements=requirements,
        provenance=(PlanProvenance(rule="test.offer", detail="test offer"),),
    )

    plan = resolve_incremental_mutation_offers(facts=facts, offers=(offer,))

    assert plan.catalog_staging == IncrementalCatalogStaging.PERMANENT_TABLE_ONLY


def test_incremental_offer_rejects_staging_that_cannot_provide_stable_reuse():
    facts = IncrementalMutationFacts(
        requested_strategy="delete+insert",
        language="sql",
        unique_key_present=True,
        requested_temp_relation_type="view",
    )
    requirements = IncrementalStrategyRequirements(
        unique_key=IncrementalUniqueKeyRequirement.OPTIONAL,
        source_consistency=IncrementalSourceConsistency.STABLE_REUSE,
        allowed_temp_relation_types=(IncrementalTempRelationType.TABLE,),
        default_temp_relation_type=IncrementalTempRelationType.TABLE,
    )
    offer = IncrementalMutationStrategyOffer.available(
        strategy=IncrementalMutationStrategy.DELETE_INSERT,
        renderer_macro="get_incremental_delete_insert_sql",
        atomicity=DdlAtomicity.UNKNOWN,
        requirements=requirements,
        provenance=(PlanProvenance(rule="test.offer", detail="test offer"),),
    )

    plan = resolve_incremental_mutation_offers(facts=facts, offers=(offer,))

    assert plan.strategy == IncrementalMutationStrategy.UNSUPPORTED
    assert "only supports temporary relation types [table]" in plan.reason


@pytest.mark.parametrize(
    "kwargs,exception,error",
    [
        ({"target_relation": None}, ValueError, "target relation"),
        ({"temp_relation": None}, ValueError, "temporary relation"),
        ({"unique_key": ""}, ValueError, "unique key"),
        ({"unique_key": []}, ValueError, "unique key columns"),
        ({"incremental_predicates": [""]}, ValueError, "predicates"),
        ({"incremental_predicates": "id > 1"}, TypeError, "sequence"),
    ],
)
def test_incremental_arguments_reject_invalid_inputs(kwargs, exception, error):
    values = {
        "target_relation": object(),
        "temp_relation": object(),
        "unique_key": None,
        "dest_columns": [],
        "incremental_predicates": None,
    }
    values.update(kwargs)

    with pytest.raises(exception, match=error):
        IncrementalMutationArguments.from_values(**values)
