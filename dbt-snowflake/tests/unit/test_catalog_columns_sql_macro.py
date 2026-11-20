import os
import re
import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


class TestSnowflakeGetCatalogColumnsSqlMacro(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do"],
        )
        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "return": lambda r: r,
        }

    def _get_template(self):
        return self.jinja_env.get_template("catalog.sql", globals=self.default_context)

    def _render_columns_sql(self, information_schema="test_db.information_schema"):
        template = self._get_template()
        sql = template.module.snowflake__get_catalog_columns_sql(information_schema)
        return re.sub(r"\s+", " ", sql.strip())

    def test_macros_load(self):
        self.jinja_env.get_template("catalog.sql")

    def test_composes_number_precision_and_scale(self):
        sql = self._render_columns_sql()

        self.assertIn(
            "when data_type = 'NUMBER' and numeric_precision is not null "
            "then 'NUMBER(' || numeric_precision || ',' || coalesce(numeric_scale, 0) || ')'",
            sql,
        )
        self.assertIn('end as "column_type"', sql)
        self.assertNotIn('data_type as "column_type"', sql)

    def test_selects_from_information_schema_columns(self):
        sql = self._render_columns_sql("analytics.information_schema")

        self.assertIn("from analytics.information_schema.columns", sql)
        self.assertIn('column_name as "column_name"', sql)
        self.assertIn('ordinal_position as "column_index"', sql)
        self.assertIn('comment as "column_comment"', sql)
