from types import SimpleNamespace

import pytest

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.planning import (
    CatalogBindingState,
    CatalogFacts,
    CreateFromQueryFacts,
    CreateFromQueryPlan,
    CreateFromQueryStrategy,
    DdlAtomicity,
    FormatFacts,
    PlanProvenance,
    RelationFacts,
    RuntimeFacts,
)


PROVENANCE = (
    PlanProvenance(
        rule="test.create_from_query",
        detail="Test operation capability selected this strategy",
    ),
)

FACTS = CreateFromQueryFacts(
    relation=RelationFacts(
        database="analytics",
        schema="mart",
        identifier="orders",
        relation_type="table",
    ),
    catalog=CatalogFacts(state=CatalogBindingState.UNBOUND),
    format=FormatFacts(),
    runtime=RuntimeFacts(engine="test"),
)


class PlanningAdapter:
    _create_from_query_fact_value = staticmethod(BaseAdapter._create_from_query_fact_value)
    get_create_from_query_runtime_facts = BaseAdapter.get_create_from_query_runtime_facts
    build_create_from_query_facts = BaseAdapter.build_create_from_query_facts
    resolve_create_from_query_plan = BaseAdapter.resolve_create_from_query_plan

    catalog_relation = None

    @classmethod
    def type(cls):
        return "test"

    def build_catalog_relation(self, model):
        return self.catalog_relation


class TemporaryAwarePlanningAdapter(PlanningAdapter):
    def get_create_from_query_runtime_facts(self, temporary, relation, model):
        engine = "temporary-runtime" if temporary else "permanent-runtime"
        return RuntimeFacts(engine=engine, version="1.0")


def test_base_adapter_resolves_portable_ctas_plan():
    relation = SimpleNamespace(
        database="analytics",
        schema="mart",
        identifier="orders",
        type="table",
        catalog=None,
    )
    adapter = PlanningAdapter()

    plan = BaseAdapter.plan_create_from_query(
        adapter, temporary=False, relation=relation, model=None
    )

    assert plan == CreateFromQueryPlan.ctas(
        temporary=False,
        atomicity=DdlAtomicity.UNKNOWN,
        facts=FACTS,
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
        facts=FACTS,
        provenance=PROVENANCE,
    )

    assert plan.to_dict() == {
        "strategy": "create_then_insert",
        "atomicity": "transaction",
        "temporary": True,
        "facts": {
            "relation": {
                "database": "analytics",
                "schema": "mart",
                "identifier": "orders",
                "relation_type": "table",
            },
            "catalog": {
                "state": "unbound",
                "integration_name": None,
                "catalog_type": None,
                "catalog_name": None,
                "catalog_database": None,
                "external_volume": None,
            },
            "format": {"table_format": None, "file_format": None},
            "runtime": {"engine": "test", "version": None},
        },
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
            facts=FACTS,
            provenance=PROVENANCE,
        )

    with pytest.raises(ValueError, match="cannot promise atomicity"):
        CreateFromQueryPlan(
            strategy=CreateFromQueryStrategy.UNSUPPORTED,
            atomicity=DdlAtomicity.STATEMENT,
            temporary=False,
            facts=FACTS,
            provenance=PROVENANCE,
            reason="CTAS and create-then-insert are unavailable",
        )


def test_supported_plan_cannot_include_unsupported_reason():
    with pytest.raises(ValueError, match="cannot include an unsupported reason"):
        CreateFromQueryPlan(
            strategy=CreateFromQueryStrategy.CTAS,
            atomicity=DdlAtomicity.STATEMENT,
            temporary=False,
            facts=FACTS,
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
            facts=FACTS,
            provenance=provenance,
        )


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("strategy", "ctas", "strategy"),
        ("atomicity", "statement", "atomicity"),
        ("temporary", 1, "temporary"),
        ("facts", {}, "facts"),
        ("reason", 1, "reason"),
    ],
)
def test_plan_rejects_untyped_fields(field, value, error):
    kwargs = {
        "strategy": CreateFromQueryStrategy.CTAS,
        "atomicity": DdlAtomicity.STATEMENT,
        "temporary": False,
        "facts": FACTS,
        "provenance": PROVENANCE,
    }
    kwargs[field] = value

    with pytest.raises(TypeError, match=error):
        CreateFromQueryPlan(**kwargs)


def test_provenance_requires_string_fields():
    with pytest.raises(TypeError, match="must be strings"):
        PlanProvenance(rule=1, detail="Invalid rule type")


def test_builds_canonical_facts_from_resolved_adapter_objects():
    adapter = PlanningAdapter()
    adapter.catalog_relation = SimpleNamespace(
        catalog_type="ICEBERG_REST",
        catalog_name="platform_catalog",
        catalog_database="catalog_db",
        external_volume="analytics_volume",
        table_format="ICEBERG",
        file_format="PARQUET",
    )
    relation = SimpleNamespace(
        database="analytics",
        schema="mart",
        identifier="orders",
        type=SimpleNamespace(value="TABLE"),
        catalog=None,
    )
    model = SimpleNamespace(config={"catalog_name": "analytics_catalog"})

    facts = adapter.build_create_from_query_facts(False, relation, model)

    assert facts.to_dict() == {
        "relation": {
            "database": "analytics",
            "schema": "mart",
            "identifier": "orders",
            "relation_type": "table",
        },
        "catalog": {
            "state": "resolved",
            "integration_name": "analytics_catalog",
            "catalog_type": "iceberg_rest",
            "catalog_name": "platform_catalog",
            "catalog_database": "catalog_db",
            "external_volume": "analytics_volume",
        },
        "format": {"table_format": "iceberg", "file_format": "parquet"},
        "runtime": {"engine": "test", "version": None},
    }


def test_builds_named_catalog_facts_without_model_resolution():
    adapter = PlanningAdapter()
    relation = SimpleNamespace(
        database="analytics",
        schema="mart",
        identifier="orders",
        type="table",
        catalog="analytics_catalog",
    )

    facts = adapter.build_create_from_query_facts(False, relation)

    assert facts.catalog == CatalogFacts(
        state=CatalogBindingState.NAMED,
        integration_name="analytics_catalog",
    )


def test_fact_hooks_receive_temporary_operation_context():
    adapter = TemporaryAwarePlanningAdapter()
    relation = SimpleNamespace(
        database="analytics",
        schema="mart",
        identifier="orders",
        type="table",
        catalog=None,
    )

    facts = adapter.build_create_from_query_facts(True, relation)

    assert facts.runtime == RuntimeFacts(engine="temporary-runtime", version="1.0")


@pytest.mark.parametrize(
    "factory,error_type,error",
    [
        (lambda: RuntimeFacts(engine=1), TypeError, "engine"),  # type: ignore[arg-type]
        (lambda: RuntimeFacts(engine=""), ValueError, "non-empty"),
        (
            lambda: RelationFacts(
                database=1,  # type: ignore[arg-type]
                schema="mart",
                identifier="orders",
                relation_type="table",
            ),
            TypeError,
            "database",
        ),
    ],
)
def test_fact_records_reject_invalid_string_fields(factory, error_type, error):
    with pytest.raises(error_type, match=error):
        factory()


@pytest.mark.parametrize(
    "kwargs,error",
    [
        (
            {
                "state": CatalogBindingState.UNBOUND,
                "integration_name": "analytics_catalog",
            },
            "Unbound",
        ),
        (
            {"state": CatalogBindingState.NAMED},
            "require an integration name",
        ),
        (
            {
                "state": CatalogBindingState.NAMED,
                "integration_name": "analytics_catalog",
                "catalog_type": "iceberg_rest",
            },
            "cannot include resolved",
        ),
        (
            {"state": CatalogBindingState.RESOLVED},
            "require a catalog type",
        ),
    ],
)
def test_catalog_facts_reject_contradictory_states(kwargs, error):
    with pytest.raises(ValueError, match=error):
        CatalogFacts(**kwargs)
