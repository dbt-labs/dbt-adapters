from multiprocessing import get_context

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base.relation import ApproximateMatchError
from dbt.adapters.snowflake import SnowflakeAdapter
from dbt.adapters.snowflake.relation import SnowflakeRelation

from .utils import config_from_parts_or_dicts

LINKED_DB = "CLD"


@pytest.fixture
def adapter():
    profile_cfg = {
        "outputs": {
            "test": {
                "type": "snowflake",
                "account": "test_account",
                "user": "test_user",
                "database": "test_database",
                "warehouse": "test_warehouse",
                "schema": "public",
            },
        },
        "target": "test",
    }
    project_cfg = {
        "name": "X",
        "version": "0.1",
        "profile": "test",
        "project-root": "/tmp/dbt/does-not-exist",
        "quoting": {"identifier": False, "schema": False, "database": False},
        "config-version": 2,
    }
    config = config_from_parts_or_dicts(project_cfg, profile_cfg)
    adapter = SnowflakeAdapter(config, get_context("spawn"))
    adapter._catalog_linked_databases.add(LINKED_DB)
    return adapter


def relation(database, schema, identifier):
    return SnowflakeRelation.create(
        database=database,
        schema=schema,
        identifier=identifier,
        quote_policy={"database": True, "schema": True, "identifier": True},
    )


def record_variants(adapter, database, schema, identifier):
    adapter._case_variant_relations.add(
        (database.casefold(), schema.casefold(), identifier.casefold())
    )


def test_non_linked_database_is_untouched(adapter):
    """The gate: a database not backed by an external catalog keeps stock behavior exactly."""
    relations = [relation("OTHER", "SCHEMA_A", "T_ORDERS")]
    record_variants(adapter, "OTHER", "SCHEMA_A", "T_ORDERS")

    assert adapter._make_match(relations, "OTHER", "SCHEMA_A", "T_ORDERS") == relations


def test_raises_on_case_variants_even_when_one_matches_exactly(adapter):
    """A-05. Stock dbt binds to the folded name and ignores its twin; we refuse instead.

    The exact match is not a safe answer: the twin is equally plausible as the model's target,
    so choosing either could read or overwrite the wrong object.
    """
    relations = [
        relation(LINKED_DB, "SCHEMA_A", "T_ORDERS"),
        relation(LINKED_DB, "SCHEMA_A", "t_orders"),
    ]
    record_variants(adapter, LINKED_DB, "SCHEMA_A", "T_ORDERS")

    with pytest.raises(DbtRuntimeError, match="differs only by case"):
        adapter._make_match(relations, LINKED_DB, "SCHEMA_A", "T_ORDERS")


def test_raises_before_matching_is_attempted(adapter):
    """The refusal precedes the match, so it fires even when nothing matches exactly."""
    relations = [
        relation(LINKED_DB, "SCHEMA_A", "Orders"),
        relation(LINKED_DB, "SCHEMA_A", "orders"),
    ]
    record_variants(adapter, LINKED_DB, "SCHEMA_A", "ORDERS")

    with pytest.raises(DbtRuntimeError, match="differs only by case"):
        adapter._make_match(relations, LINKED_DB, "SCHEMA_A", "ORDERS")


def test_exact_match_still_wins_when_unambiguous(adapter):
    exact = relation(LINKED_DB, "SCHEMA_A", "T_ORDERS")
    other = relation(LINKED_DB, "SCHEMA_A", "T_OTHER")

    assert adapter._make_match([exact, other], LINKED_DB, "SCHEMA_A", "T_ORDERS") == [exact]


def test_variants_recorded_for_a_different_identifier_do_not_block(adapter):
    relations = [relation(LINKED_DB, "SCHEMA_A", "T_ORDERS")]
    record_variants(adapter, LINKED_DB, "SCHEMA_A", "T_CUSTOMERS")

    assert adapter._make_match(relations, LINKED_DB, "SCHEMA_A", "T_ORDERS") == relations


def test_resolves_identifier_differing_only_by_case(adapter):
    relations = [relation(LINKED_DB, "SCHEMA_A", "t_orders")]

    assert adapter._make_match(relations, LINKED_DB, "SCHEMA_A", "T_ORDERS") == relations


def test_resolves_schema_differing_only_by_case(adapter):
    """A-08: the schema alone can be the driver, with the relation identifier matching exactly."""
    relations = [relation(LINKED_DB, "schema_a", "T_ORDERS")]

    assert adapter._make_match(relations, LINKED_DB, "SCHEMA_A", "T_ORDERS") == relations


def test_non_linked_database_still_raises_on_a_case_mismatch(adapter):
    """The contrast with the two tests above: off a catalog-linked database, stock behavior holds."""
    relations = [relation("OTHER", "SCHEMA_A", "t_orders")]

    with pytest.raises(ApproximateMatchError):
        adapter._make_match(relations, "OTHER", "SCHEMA_A", "T_ORDERS")


def test_no_match_returns_empty_rather_than_raising(adapter):
    relations = [relation(LINKED_DB, "SCHEMA_A", "T_ORDERS")]

    assert adapter._make_match(relations, LINKED_DB, "SCHEMA_A", "T_MISSING") == []
