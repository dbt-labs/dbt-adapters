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


class TestDynamicIcebergTableBaseLocation(unittest.TestCase):
    """Snowflake-managed storage Iceberg tables reject BASE_LOCATION, so the catalog relation
    leaves base_location unset for them. Both the create and the replace DDL for dynamic
    Iceberg tables must then omit the clause instead of rendering it."""

    def _module(self, path, base_location, external_volume):
        jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do"],
        )
        catalog_relation = SimpleNamespace(
            catalog_type="BUILT_IN",
            external_volume=external_volume,
            base_location=base_location,
            iceberg_version=None,
        )
        adapter = mock.Mock()
        adapter.build_catalog_relation = lambda model: catalog_relation
        context = {
            "adapter": adapter,
            "config": mock.Mock(),
            "exceptions": mock.Mock(),
        }
        optional_template = jinja_env.get_template("utils/optional.sql", globals=context)
        context["optional"] = optional_template.module.optional
        return jinja_env.get_template(path, globals=context).module

    @staticmethod
    def _dynamic_table():
        return SnowflakeDynamicTableConfig(
            name="my_dynamic_table",
            schema_name="my_schema",
            database_name="my_database",
            query="select 1 as id",
            snowflake_warehouse="my_warehouse",
            target_lag="1 minute",
        )

    def _render(self, path, macro_name, base_location, external_volume):
        module = self._module(path, base_location, external_volume)
        sql = getattr(module, macro_name)(
            self._dynamic_table(),
            "my_database.my_schema.my_dynamic_table",
            "select 1 as id",
        )
        return re.sub(r"\s+", " ", sql).strip().lower()

    def _render_create(self, base_location, external_volume):
        return self._render(
            "relations/dynamic_table/create.sql",
            "snowflake__create_dynamic_table_built_in_sql",
            base_location,
            external_volume,
        )

    def _render_replace(self, base_location, external_volume):
        return self._render(
            "relations/dynamic_table/replace.sql",
            "snowflake__replace_dynamic_table_built_in_sql",
            base_location,
            external_volume,
        )

    def test_replace_omits_base_location_for_snowflake_managed_storage(self):
        sql = self._render_replace(base_location=None, external_volume="SNOWFLAKE_MANAGED")

        self.assertIn("create or replace dynamic iceberg table", sql)
        self.assertIn("external_volume = 'snowflake_managed'", sql)
        self.assertNotIn("base_location", sql)
        self.assertNotIn("none", sql)

    def test_create_omits_base_location_for_snowflake_managed_storage(self):
        sql = self._render_create(base_location=None, external_volume="SNOWFLAKE_MANAGED")

        self.assertIn("create dynamic iceberg table", sql)
        self.assertNotIn("base_location", sql)
        self.assertNotIn("none", sql)

    def test_replace_renders_base_location_for_external_volume(self):
        sql = self._render_replace(
            base_location="_dbt/my_schema/my_dynamic_table", external_volume="my_volume"
        )

        self.assertIn("external_volume = 'my_volume'", sql)
        self.assertIn("base_location = '_dbt/my_schema/my_dynamic_table'", sql)

    def test_create_renders_base_location_for_external_volume(self):
        sql = self._render_create(
            base_location="_dbt/my_schema/my_dynamic_table", external_volume="my_volume"
        )

        self.assertIn("external_volume = 'my_volume'", sql)
        self.assertIn("base_location = '_dbt/my_schema/my_dynamic_table'", sql)
