from dataclasses import dataclass

import pytest

from dbt.adapters.base import BaseRelation, Column
from dbt.adapters.sql import SQLAdapter


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("plain", '"plain"'),
        ("with space", '"with space"'),
        ('a"b', '"a""b"'),
        ('"', '""""'),
        ('""', '""""""'),
        ('a" text; drop table t; --', '"a"" text; drop table t; --"'),
    ],
)
def test_embedded_quote_is_escaped(identifier, expected):
    assert SQLAdapter.quote(identifier) == expected
    assert Column(identifier, "text").quoted == expected
    assert BaseRelation.create(identifier=identifier).render() == expected


def test_relation_escapes_every_quoted_part():
    relation = BaseRelation.create(database='d"b', schema='s"c', identifier='i"d')
    assert relation.render() == '"d""b"."s""c"."i""d"'


def test_relation_unquoted_parts_are_rendered_as_is():
    relation = BaseRelation.create(schema="sch", identifier='i"d').quote(identifier=False)
    assert relation.render() == '"sch".i"d'


@dataclass(frozen=True, eq=False, repr=False)
class BacktickRelation(BaseRelation):
    quote_character: str = "`"


def test_relation_escapes_its_own_quote_character():
    relation = BacktickRelation.create(schema="s`c", identifier='i"d')
    assert relation.render() == '`s``c`.`i"d`'


@dataclass(frozen=True, eq=False, repr=False)
class NoQuoteRelation(BaseRelation):
    quote_character: str = ""


def test_relation_without_quote_character_is_unchanged():
    relation = NoQuoteRelation.create(schema="sch", identifier='i"d')
    assert relation.render() == 'sch.i"d'
