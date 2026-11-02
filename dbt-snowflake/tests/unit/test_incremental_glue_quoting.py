import os
import unittest
from types import SimpleNamespace

from dbt_common.clients.jinja import get_environment

from dbt.adapters.snowflake.column import SnowflakeColumn

INCREMENTAL_SQL = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "../../src/dbt/include/snowflake/macros/materializations/incremental.sql",
    )
)
GLUE = SimpleNamespace(catalog_linked_database_type="glue")


class _MacroReturn(Exception):
    """Stand-in for dbt's `return`, which unwinds the macro rather than emitting a value."""

    def __init__(self, value):
        self.value = value


class TestSnowflakeQuoteGlueColumns(unittest.TestCase):
    def setUp(self):
        def _return(value):
            raise _MacroReturn(value)

        with open(INCREMENTAL_SQL) as f:
            self.module = (
                get_environment().from_string(f.read(), globals={"return": _return}).module
            )
        self.dest_columns = [
            SnowflakeColumn("id", "FIXED"),
            SnowflakeColumn("order_id", "TEXT"),
        ]

    def _quote(self, names, catalog_relation=GLUE):
        try:
            self.module.dbt_macro__snowflake__quote_glue_columns(
                names, self.dest_columns, catalog_relation
            )
        except _MacroReturn as ret:
            return ret.value
        self.fail("macro did not return")

    def test_name_of_a_column_is_quoted_as_reported(self):
        self.assertEqual(self._quote("id"), '"id"')
        self.assertEqual(self._quote("ID"), '"id"')

    def test_each_name_in_a_list_is_quoted(self):
        self.assertEqual(self._quote(["id", "Order_Id"]), ['"id"', '"order_id"'])

    def test_quoted_names_and_expressions_pass_through(self):
        self.assertEqual(self._quote('"id"'), '"id"')
        self.assertEqual(self._quote("id || '-' || order_id"), "id || '-' || order_id")

    def test_names_outside_glue_pass_through(self):
        for catalog_relation in (
            None,
            SimpleNamespace(),
            SimpleNamespace(catalog_linked_database_type=None),
        ):
            self.assertEqual(self._quote(["id"], catalog_relation), ["id"])
