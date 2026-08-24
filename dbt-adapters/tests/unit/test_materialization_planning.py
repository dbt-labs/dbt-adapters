from unittest.mock import MagicMock

import pytest

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.planning import (
    ExistingIndexStrategy,
    MaterializationHookStrategy,
    MaterializationStatementStrategy,
    MaterializationTransactionStrategy,
    PlanProvenance,
    TableDocumentationStrategy,
    TableIndexStrategy,
    TableLifecyclePlan,
    TableReplacementStrategy,
)


def _adapter() -> MagicMock:
    adapter = MagicMock(spec=BaseAdapter)
    adapter.plan_table_materialization = BaseAdapter.plan_table_materialization.__get__(
        adapter
    )
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
        "provenance": [
            {
                "rule": "materialization.table.default",
                "detail": (
                    "Built-in SQL table materialization uses stage-and-swap replacement"
                ),
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
    assert (
        BaseAdapter.plan_table_materialization(_adapter(), macro_id, language) is None
    )


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
