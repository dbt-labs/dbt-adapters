from dataclasses import dataclass, replace

import pytest

from dbt.adapters.base import BaseRelation
from dbt.adapters.contracts.relation import RelationType


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


def test_create_ephemeral_from_uses_identifier():
    @dataclass
    class Node:
        """Dummy implementation of RelationConfig protocol"""

        name: str
        identifier: str

    node = Node(name="name_should_not_be_used", identifier="test")
    ephemeral_relation = BaseRelation.create_ephemeral_from(node)
    assert str(ephemeral_relation) == "__dbt__cte__test"
