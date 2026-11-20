from dataclasses import dataclass, replace
from datetime import datetime
import pytest

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.relation import EventTimeFilter
from dbt.adapters.contracts.relation import RelationType


@pytest.mark.parametrize(
    "relation_type,check_property",
    [
        (RelationType.CTE, "is_cte"),
        (RelationType.MaterializedView, "is_materialized_view"),
        (RelationType.PointerTable, "is_pointer"),
        (RelationType.Table, "is_table"),
        (RelationType.View, "is_view"),
    ],
)
def test_relation_types(relation_type, check_property):
    my_relation = BaseRelation.create(
        "fake_database",
        "fake_schema",
        "fake_identifier",
        type=relation_type,
    )
    assert getattr(my_relation, check_property)
    assert my_relation.type == relation_type


@pytest.mark.parametrize(
    "relation_type,result",
    [
        (RelationType.View, True),
        (RelationType.External, False),
    ],
)
def test_can_be_renamed(relation_type, result):
    my_relation = BaseRelation.create(type=relation_type)
    my_relation = replace(my_relation, renameable_relations=frozenset({RelationType.View}))
    assert my_relation.can_be_renamed is result


def test_can_be_renamed_default():
    my_relation = BaseRelation.create(type=RelationType.View)
    assert my_relation.can_be_renamed is False


@pytest.mark.parametrize(
    "relation_type,result",
    [
        (RelationType.View, True),
        (RelationType.External, False),
    ],
)
def test_can_be_replaced(relation_type, result):
    my_relation = BaseRelation.create(type=relation_type)
    my_relation = replace(my_relation, replaceable_relations=frozenset({RelationType.View}))
    assert my_relation.can_be_replaced is result


def test_can_be_replaced_default():
    my_relation = BaseRelation.create(type=RelationType.View)
    assert my_relation.can_be_replaced is False


@pytest.mark.parametrize(
    "limit,require_alias,expected_result",
    [
        (None, False, '"test_database"."test_schema"."test_identifier"'),
        (
            0,
            True,
            '(select * from "test_database"."test_schema"."test_identifier" where false limit 0) _dbt_limit_subq_test_identifier',
        ),
        (
            1,
            True,
            '(select * from "test_database"."test_schema"."test_identifier" limit 1) _dbt_limit_subq_test_identifier',
        ),
        (
            0,
            False,
            '(select * from "test_database"."test_schema"."test_identifier" where false limit 0)',
        ),
        (
            1,
            False,
            '(select * from "test_database"."test_schema"."test_identifier" limit 1)',
        ),
    ],
)
def test_render_limited(limit, require_alias, expected_result):
    my_relation = BaseRelation.create(
        database="test_database",
        schema="test_schema",
        identifier="test_identifier",
        limit=limit,
        require_alias=require_alias,
    )
    actual_result = my_relation.render_limited()
    assert actual_result == expected_result
    assert str(my_relation) == expected_result


@pytest.mark.parametrize(
    "event_time_filter,require_alias,expected_result",
    [
        (None, False, '"test_database"."test_schema"."test_identifier"'),
        (
            EventTimeFilter(field_name="column"),
            False,
            '"test_database"."test_schema"."test_identifier"',
        ),
        (None, True, '"test_database"."test_schema"."test_identifier"'),
        (
            EventTimeFilter(field_name="column"),
            True,
            '"test_database"."test_schema"."test_identifier"',
        ),
        (
            EventTimeFilter(field_name="column", start=datetime(year=2020, month=1, day=1)),
            False,
            """(select * from "test_database"."test_schema"."test_identifier" where column >= '2020-01-01 00:00:00')""",
        ),
        (
            EventTimeFilter(field_name="column", start=datetime(year=2020, month=1, day=1)),
            True,
            """(select * from "test_database"."test_schema"."test_identifier" where column >= '2020-01-01 00:00:00') _dbt_et_filter_subq_test_identifier""",
        ),
        (
            EventTimeFilter(field_name="column", end=datetime(year=2020, month=1, day=1)),
            False,
            """(select * from "test_database"."test_schema"."test_identifier" where column < '2020-01-01 00:00:00')""",
        ),
        (
            EventTimeFilter(
                field_name="column",
                start=datetime(year=2020, month=1, day=1),
                end=datetime(year=2020, month=1, day=2),
            ),
            False,
            """(select * from "test_database"."test_schema"."test_identifier" where column >= '2020-01-01 00:00:00' and column < '2020-01-02 00:00:00')""",
        ),
    ],
)
def test_render_event_time_filtered(event_time_filter, require_alias, expected_result):
    my_relation = BaseRelation.create(
        database="test_database",
        schema="test_schema",
        identifier="test_identifier",
        event_time_filter=event_time_filter,
        require_alias=require_alias,
    )
    actual_result = my_relation.render_event_time_filtered()
    assert actual_result == expected_result
    assert str(my_relation) == expected_result


def test_render_event_time_filtered_and_limited():
    my_relation = BaseRelation.create(
        database="test_database",
        schema="test_schema",
        identifier="test_identifier",
        event_time_filter=EventTimeFilter(
            field_name="column",
            start=datetime(year=2020, month=1, day=1),
            end=datetime(year=2020, month=1, day=2),
        ),
        limit=0,
        require_alias=False,
    )
    expected_result = """(select * from (select * from "test_database"."test_schema"."test_identifier" where false limit 0) where column >= '2020-01-01 00:00:00' and column < '2020-01-02 00:00:00')"""

    actual_result = my_relation.render_event_time_filtered(my_relation.render_limited())
    assert actual_result == expected_result
    assert str(my_relation) == expected_result


def test_create_ephemeral_from_uses_identifier():
    @dataclass
    class Node:
        """Dummy implementation of RelationConfig protocol"""

        name: str
        identifier: str

    node = Node(name="name_should_not_be_used", identifier="test")
    ephemeral_relation = BaseRelation.create_ephemeral_from(node)
    assert str(ephemeral_relation) == "__dbt__cte__test"


def _db_only(database: str, schema: str, identifier: str) -> BaseRelation:
    """Mirror what dbt-core RunTask.create_schemas does for each model."""
    full = BaseRelation.create(
        database=database,
        schema=schema,
        identifier=identifier,
        type=RelationType.Table,
    )
    return full.include(database=True, schema=False, identifier=False)


def test_db_only_relations_dedupe_in_a_set():
    """
    Relations that render to the same string must compare equal so that
    `Set[BaseRelation]` deduplicates them.

    Regression test: `RunTask.create_schemas` in dbt-core builds
    `required_databases: Set[BaseRelation]` from
    `required.include(database=True, schema=False, identifier=False)`.
    When this dedup fails, dbt fans out one `list_schemas(database)` future
    per (database, schema) pair instead of one per unique database, causing
    N redundant `information_schema.schemata` queries on engines like Trino.
    """
    relations = [_db_only("hive", f"schema_{i}", f"table_{i}") for i in range(5)]
    assert {r.render() for r in relations} == {'"hive"'}
    assert len({hash(r) for r in relations}) == 1
    assert relations[0] == relations[1]
    assert len(set(relations)) == 1


def test_relation_eq_matches_hash():
    """`a == b` must imply `hash(a) == hash(b)`, and the practically harmful
    inverse (`hash(a) == hash(b) and a != b`) must not be reachable for
    relations that render identically."""
    a = _db_only("hive", "schema_a", "table_a")
    b = _db_only("hive", "schema_b", "table_b")
    assert hash(a) == hash(b)
    assert a == b
