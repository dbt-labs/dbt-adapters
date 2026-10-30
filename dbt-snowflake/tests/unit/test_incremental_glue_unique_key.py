import os
import unittest

from dbt_common.clients.jinja import get_environment

from dbt.adapters.snowflake.column import SnowflakeColumn

INCREMENTAL_SQL = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "../../src/dbt/include/snowflake/macros/materializations/incremental.sql",
    )
)


class _MacroReturn(Exception):
    """Stand-in for dbt's `return`, which unwinds the macro rather than emitting a value."""

    def __init__(self, value):
        self.value = value


class TestSnowflakeQuoteGlueUniqueKey(unittest.TestCase):
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

    def _quote(self, unique_key):
        try:
            self.module.dbt_macro__snowflake__quote_glue_unique_key(unique_key, self.dest_columns)
        except _MacroReturn as ret:
            return ret.value
        self.fail("macro did not return")

    def test_key_naming_a_column_is_quoted_as_reported(self):
        self.assertEqual(self._quote("id"), '"id"')
        self.assertEqual(self._quote("ID"), '"id"')

    def test_each_key_in_a_list_is_quoted(self):
        self.assertEqual(self._quote(["id", "Order_Id"]), ['"id"', '"order_id"'])

    def test_quoted_keys_and_expressions_pass_through(self):
        self.assertEqual(self._quote('"id"'), '"id"')
        self.assertEqual(self._quote("id || '-' || order_id"), "id || '-' || order_id")
