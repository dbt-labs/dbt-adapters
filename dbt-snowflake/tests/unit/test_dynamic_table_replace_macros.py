import os
import re
import unittest
from types import SimpleNamespace
from unittest import mock

from jinja2 import Environment, FileSystemLoader

from dbt.adapters.snowflake.relation_configs.dynamic_table import (
    SnowflakeDynamicTableConfig,
)

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


class TestSnowflakeReplaceDynamicTableMacros(unittest.TestCase):
    """Renders the real CREATE OR REPLACE DYNAMIC TABLE DDL from replace.sql.

    Snowflake's grammar places COPY GRANTS after CLUSTER BY and before the
    ROW ACCESS POLICY and TAG clauses:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#syntax
    """

    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do"],
        )
        catalog_relation = SimpleNamespace(
            catalog_type="BUILT_IN",
            external_volume="my_volume",
            base_location="my/base/location",
            iceberg_version=None,
        )
        adapter = mock.Mock()
        adapter.build_catalog_relation = lambda model: catalog_relation
        adapter.behavior.snowflake_default_transient_dynamic_tables.no_warn = False
        self.context = {
            "adapter": adapter,
            "config": mock.Mock(),
            "exceptions": mock.Mock(),
        }
        optional_template = self.jinja_env.get_template("utils/optional.sql", globals=self.context)
        self.context["optional"] = optional_template.module.optional
        self.module = self.jinja_env.get_template(
            "relations/dynamic_table/replace.sql", globals=self.context
        ).module

    @staticmethod
    def _config(**kwargs):
        return SnowflakeDynamicTableConfig(
            name="my_dynamic_table",
            schema_name="my_schema",
            database_name="my_database",
            query="select 1 as id",
            snowflake_warehouse="my_warehouse",
            target_lag="1 minute",
            **kwargs,
        )

    def _render(self, macro_name, dynamic_table):
        sql = getattr(self.module, macro_name)(
            dynamic_table, "my_database.my_schema.my_dynamic_table", "select 1 as id"
        )
        return re.sub(r"\s+", " ", sql).strip().lower()

    def _assert_clause_order(self, sql, clauses):
        positions = [sql.index(clause) for clause in clauses]
        self.assertEqual(positions, sorted(positions), sql)

    def test_info_schema_copy_grants_precedes_policy_and_tag(self):
        dynamic_table = self._config(
            copy_grants=True,
            cluster_by="id",
            row_access_policy="my_policy on (id)",
            table_tag="env = 'prod'",
            immutable_where="id > 0",
        )
        sql = self._render("snowflake__replace_dynamic_table_info_schema_sql", dynamic_table)

        self.assertIn("create or replace dynamic table", sql)
        self._assert_clause_order(
            sql,
            (
                "cluster by (id)",
                "copy grants",
                "with row access policy my_policy on (id)",
                "with tag (env = 'prod')",
                "immutable where (id > 0)",
                "as (",
            ),
        )

    def test_info_schema_copy_grants_alone(self):
        dynamic_table = self._config(copy_grants=True)
        sql = self._render("snowflake__replace_dynamic_table_info_schema_sql", dynamic_table)

        self.assertIn("copy grants as (", sql)
        self.assertNotIn("cluster by", sql)
        self.assertNotIn("with tag", sql)

    def test_info_schema_without_copy_grants(self):
        dynamic_table = self._config(table_tag="env = 'prod'")
        sql = self._render("snowflake__replace_dynamic_table_info_schema_sql", dynamic_table)

        self.assertNotIn("copy grants", sql)
        self.assertIn("with tag (env = 'prod') as (", sql)

    def test_built_in_copy_grants_precedes_policy_and_tag(self):
        dynamic_table = self._config(
            copy_grants=True,
            cluster_by="id",
            row_access_policy="my_policy on (id)",
            table_tag="env = 'prod'",
            immutable_where="id > 0",
        )
        sql = self._render("snowflake__replace_dynamic_table_built_in_sql", dynamic_table)

        self.assertIn("create or replace dynamic iceberg table", sql)
        self._assert_clause_order(
            sql,
            (
                "cluster by (id)",
                "copy grants",
                "row_access_policy",
                "table_tag",
                "immutable where (id > 0)",
                "as (",
            ),
        )
