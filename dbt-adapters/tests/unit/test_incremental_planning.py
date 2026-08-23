from types import SimpleNamespace

import pytest

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.planning import (
    DdlAtomicity,
    IncrementalMutationStrategy,
    resolve_incremental_mutation_plan,
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
