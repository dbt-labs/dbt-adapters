import pytest

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.planning import (
    CreateFromQueryPlan,
    CreateFromQueryStrategy,
    DdlAtomicity,
    PlanProvenance,
)


PROVENANCE = (
    PlanProvenance(
        rule="test.create_from_query",
        detail="Test operation capability selected this strategy",
    ),
)


def test_base_adapter_resolves_portable_ctas_plan():
    plan = BaseAdapter.plan_create_from_query(None, temporary=False, relation=None)

    assert plan == CreateFromQueryPlan.ctas(
        temporary=False,
        atomicity=DdlAtomicity.UNKNOWN,
        provenance=(
            PlanProvenance(
                rule="base.create_from_query.ctas",
                detail="Base adapter create-from-query behavior uses create table as select",
            ),
        ),
    )
    assert "plan_create_from_query" in BaseAdapter._available_


def test_plan_serializes_to_stable_primitives():
    plan = CreateFromQueryPlan.create_then_insert(
        temporary=True,
        atomicity=DdlAtomicity.TRANSACTION,
        provenance=PROVENANCE,
    )

    assert plan.to_dict() == {
        "strategy": "create_then_insert",
        "atomicity": "transaction",
        "temporary": True,
        "provenance": [
            {
                "rule": "test.create_from_query",
                "detail": "Test operation capability selected this strategy",
            }
        ],
        "reason": None,
    }


def test_unsupported_plan_requires_reason_and_cannot_promise_atomicity():
    with pytest.raises(ValueError, match="must include a reason"):
        CreateFromQueryPlan(
            strategy=CreateFromQueryStrategy.UNSUPPORTED,
            atomicity=DdlAtomicity.NONE,
            temporary=False,
            provenance=PROVENANCE,
        )

    with pytest.raises(ValueError, match="cannot promise atomicity"):
        CreateFromQueryPlan(
            strategy=CreateFromQueryStrategy.UNSUPPORTED,
            atomicity=DdlAtomicity.STATEMENT,
            temporary=False,
            provenance=PROVENANCE,
            reason="CTAS and create-then-insert are unavailable",
        )


def test_supported_plan_cannot_include_unsupported_reason():
    with pytest.raises(ValueError, match="cannot include an unsupported reason"):
        CreateFromQueryPlan(
            strategy=CreateFromQueryStrategy.CTAS,
            atomicity=DdlAtomicity.STATEMENT,
            temporary=False,
            provenance=PROVENANCE,
            reason="This state is contradictory",
        )


@pytest.mark.parametrize("provenance", [(), ("not provenance",), [PROVENANCE[0]]])
def test_plan_requires_typed_provenance(provenance):
    error = ValueError if provenance == () else TypeError

    with pytest.raises(error):
        CreateFromQueryPlan.ctas(
            temporary=False,
            atomicity=DdlAtomicity.STATEMENT,
            provenance=provenance,
        )


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("strategy", "ctas", "strategy"),
        ("atomicity", "statement", "atomicity"),
        ("temporary", 1, "temporary"),
        ("reason", 1, "reason"),
    ],
)
def test_plan_rejects_untyped_fields(field, value, error):
    kwargs = {
        "strategy": CreateFromQueryStrategy.CTAS,
        "atomicity": DdlAtomicity.STATEMENT,
        "temporary": False,
        "provenance": PROVENANCE,
    }
    kwargs[field] = value

    with pytest.raises(TypeError, match=error):
        CreateFromQueryPlan(**kwargs)


def test_provenance_requires_string_fields():
    with pytest.raises(TypeError, match="must be strings"):
        PlanProvenance(rule=1, detail="Invalid rule type")
