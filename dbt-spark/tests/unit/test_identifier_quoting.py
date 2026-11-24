from multiprocessing import get_context

import pytest

from dbt.adapters.spark import SparkAdapter, SparkColumn, SparkRelation


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("plain", "`plain`"),
        ("with space", "`with space`"),
        ("a`b", "`a``b`"),
        ('a"b', '`a"b`'),
        ("a` string, b string) --", "`a`` string, b string) --`"),
    ],
)
def test_embedded_backtick_is_escaped(target_http, identifier, expected):
    adapter = SparkAdapter(target_http, get_context("spawn"))
    assert adapter.quote(identifier) == expected
    assert SparkColumn(identifier, "string").quoted == expected
    relation = SparkRelation.create(schema="sch", identifier=identifier).quote(identifier=True)
    assert relation.render() == f"sch.{expected}"
