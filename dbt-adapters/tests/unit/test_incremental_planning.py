from types import SimpleNamespace

import pytest

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.planning import (
    DdlAtomicity,
    IncrementalMutationArguments,
    IncrementalMutationStrategy,
    IncrementalSchemaChangeStrategy,
    resolve_incremental_mutation_plan,
    resolve_incremental_schema_change_plan,
)
from dbt_common.exceptions import DbtRuntimeError


BUILTIN_STRATEGIES = ["append", "delete+insert", "merge", "insert_overwrite", "microbatch"]


@pytest.mark.parametrize(
    "requested,expected_strategy,expected_macro",
    [
        (None, IncrementalMutationStrategy.ADAPTER_DEFAULT, "get_incremental_default_sql"),
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
        "reason": "The incremental strategy 'merge' is not valid for this adapter",
    }


def test_base_adapter_resolver_uses_adapter_strategy_support():
    adapter = SimpleNamespace(
        valid_incremental_strategies=lambda: ["append"],
        builtin_incremental_strategies=lambda: BUILTIN_STRATEGIES,
    )

    plan = BaseAdapter.plan_incremental_mutation(adapter, "merge")

    assert plan.strategy == IncrementalMutationStrategy.UNSUPPORTED


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
    adapter = SimpleNamespace(
        config=SimpleNamespace(project_name="test_project"),
        get_incremental_strategy_macro=lambda context, strategy: None,
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
    }


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
