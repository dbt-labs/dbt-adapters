from types import SimpleNamespace

import jinja2
import pytest

A_COLUMN = SimpleNamespace(name="my_col")


class MacroReturn(Exception):
    """Mirrors dbt's `return()`, which unwinds the macro rather than emitting a value."""

    def __init__(self, value):
        self.value = value


def _load_macros(use_show_apis, captured_sql, columns_per_call=None):
    """Render adapters.sql with `statement` stubbed out so we can inspect the emitted SQL.

    `columns_per_call` is the list of column-lists that successive
    sql_convert_columns_in_relation() calls return, letting a test simulate the
    unqualified lookup finding nothing.
    """
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("src/dbt/include/redshift/macros"),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("adapters.sql")

    def fake_statement(name, caller=None, **kwargs):
        """Stands in for the `statement` call block: records the SQL in its body."""
        captured_sql.append(caller())
        return ""

    remaining = list(columns_per_call if columns_per_call is not None else [[A_COLUMN]])

    def fake_convert(table):
        return remaining.pop(0) if remaining else []

    def fake_return(value):
        raise MacroReturn(value)

    return template.make_module(
        {
            "statement": fake_statement,
            "load_result": lambda name: SimpleNamespace(table=[]),
            "sql_convert_columns_in_relation": fake_convert,
            "redshift__use_show_apis": lambda: use_show_apis,
            "adapter": SimpleNamespace(quote=lambda value: f'"{value}"'),
            "return": fake_return,
        }
    )


def _get_columns(macros, relation):
    try:
        macros.redshift__get_columns_in_relation(relation)
    except MacroReturn as macro_return:
        return macro_return.value
    raise AssertionError("macro did not return")


def _relation(database, schema, identifier="my_model__dbt_tmp"):
    return SimpleNamespace(database=database, schema=schema, identifier=identifier)


@pytest.mark.parametrize("use_show_apis", [True, False])
def test_temp_relation_skips_late_binding_view_lookup(use_show_apis):
    """Temp relations carry no database/schema and are never views, in either datasharing mode."""
    captured_sql = []
    macros = _load_macros(use_show_apis, captured_sql)

    columns = _get_columns(macros, _relation(database=None, schema=None))

    assert len(captured_sql) == 1
    sql = captured_sql[0]
    assert "pg_get_late_binding_view_cols" not in sql
    assert "svv_external_columns" not in sql
    assert "SHOW COLUMNS" not in sql
    assert 'from information_schema."columns"' in sql
    assert "where table_name = 'my_model__dbt_tmp'" in sql
    assert columns == [A_COLUMN]


def test_unqualified_relation_falls_back_to_legacy_when_nothing_matches():
    """Any non-temp caller that passes an unqualified relation keeps the old behavior."""
    captured_sql = []
    macros = _load_macros(False, captured_sql, columns_per_call=[[], [A_COLUMN]])

    columns = _get_columns(macros, _relation(database=None, schema=None, identifier="some_lbv"))

    assert len(captured_sql) == 2
    assert "pg_get_late_binding_view_cols" not in captured_sql[0]
    assert "pg_get_late_binding_view_cols" in captured_sql[1]
    assert columns == [A_COLUMN]


def test_regular_relation_uses_show_columns_when_datasharing_on():
    captured_sql = []
    macros = _load_macros(True, captured_sql)

    _get_columns(macros, _relation(database="db", schema="my_schema"))

    assert len(captured_sql) == 1
    assert "SHOW COLUMNS FROM TABLE" in captured_sql[0]


def test_regular_relation_uses_legacy_query_when_datasharing_off():
    captured_sql = []
    macros = _load_macros(False, captured_sql)

    _get_columns(macros, _relation(database="db", schema="my_schema"))

    assert len(captured_sql) == 1
    assert "pg_get_late_binding_view_cols" in captured_sql[0]
