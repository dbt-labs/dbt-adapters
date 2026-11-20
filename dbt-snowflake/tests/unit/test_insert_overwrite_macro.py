import os
import unittest
from types import SimpleNamespace
from unittest import mock

from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)
TEMPLATE = "materializations/incremental/insert_overwrite.sql"


class _MacroReturn(Exception):
    """Stand-in for dbt's `return`, which unwinds the macro rather than emitting a value."""

    def __init__(self, value):
        self.value = value


def _column(name):
    return SimpleNamespace(name=name)


class TestSnowflakeInsertOverwriteGetSql(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do"],
        )

        self.config = {}
        self.catalog_linked = False

        def _return(value):
            raise _MacroReturn(value)

        self.context = {
            "config": mock.Mock(),
            "return": _return,
            "get_quoted_csv": lambda names: ", ".join(f'"{name}"' for name in names),
            "snowflake__is_catalog_linked_database": lambda relation=None, catalog_relation=None: (
                self.catalog_linked
            ),
            "snowflake_dml_explicit_transaction": lambda dml: f"begin;\n{dml};\ncommit;",
        }
        self.context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )

    def _render(self, dest_columns=None, unique_key=None):
        template = self.jinja_env.get_template(TEMPLATE, globals=self.context)
        target = mock.Mock()
        target.render.return_value = "db.sch.target"
        source = mock.Mock()
        source.render.return_value = "db.sch.target__dbt_tmp"

        try:
            template.module.snowflake__insert_overwrite_get_sql(
                target, source, unique_key, dest_columns
            )
        except _MacroReturn as macro_return:
            return macro_return.value
        raise AssertionError("macro did not return")

    def test_macros_load(self):
        self.jinja_env.get_template(TEMPLATE)

    def test_dest_columns_name_both_sides(self):
        sql = self._render(dest_columns=[_column("A"), _column("B")])

        self.assertIn('insert overwrite into db.sch.target ("A", "B")', sql)
        self.assertIn('select "A", "B"', sql)
        self.assertIn("from db.sch.target__dbt_tmp", sql)

    def test_overwrite_columns_take_precedence_and_are_joined_as_given(self):
        self.config["overwrite_columns"] = ['"a"', "b"]
        sql = self._render(dest_columns=[_column("A"), _column("B"), _column("C")])

        self.assertIn('insert overwrite into db.sch.target ("a", b)', sql)
        self.assertIn('select "a", b', sql)
        self.assertNotIn('"C"', sql)

    def test_no_columns_falls_back_to_select_star(self):
        sql = self._render(dest_columns=[])

        self.assertIn("insert overwrite into db.sch.target", sql)
        self.assertIn("select *", sql)
        self.assertNotIn("(", sql.split("select")[0].replace("db.sch.target", ""))

    def test_sql_header_stays_on_its_own_line(self):
        """A header ending in a `--` comment would otherwise comment out the statement."""
        self.config["sql_header"] = "alter session set query_tag = 'x';"
        sql = self._render(dest_columns=[_column("A")])

        header_lines = [line.strip() for line in sql.splitlines() if "query_tag" in line]
        self.assertEqual(header_lines, ["alter session set query_tag = 'x';"])

    def test_wrapped_in_explicit_transaction(self):
        sql = self._render(dest_columns=[_column("A")])

        self.assertTrue(sql.startswith("begin;"))
        self.assertTrue(sql.endswith("commit;"))

    def test_catalog_linked_database_skips_transaction(self):
        self.catalog_linked = True
        sql = self._render(dest_columns=[_column("A")])

        self.assertNotIn("begin;", sql)
        self.assertIn('insert overwrite into db.sch.target ("A")', sql)
