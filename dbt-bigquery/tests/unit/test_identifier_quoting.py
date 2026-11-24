import pytest

from dbt.adapters.bigquery import BigQueryAdapter, BigQueryColumn, BigQueryRelation


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("plain", "`plain`"),
        ("with space", "`with space`"),
        ("a`b", "`a\\`b`"),
        ("a\\", "`a\\\\`"),
        ("a\\`b", "`a\\\\\\`b`"),
        ('a"b', '`a"b`'),
    ],
)
def test_embedded_backtick_and_backslash_are_escaped(identifier, expected):
    assert BigQueryAdapter.quote(identifier) == expected
    assert BigQueryColumn(identifier, "STRING").quoted == expected
    relation = BigQueryRelation.create(database="proj", schema="ds", identifier=identifier)
    assert relation.render() == f"`proj`.`ds`.{expected}"


def test_information_schema_escapes_backtick():
    relation = BigQueryRelation.create(database="pro`j", schema="ds", identifier="tbl")
    rendered = relation.information_schema("TABLES").render()
    assert rendered.startswith("`pro\\`j`.")
