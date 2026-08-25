import os
import re
import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader

from dbt.adapters.snowflake.relation_configs.interactive_table import (
    SnowflakeInteractiveTableConfig,
)

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


class TestSnowflakeInteractiveTableCreateReplaceMacros(unittest.TestCase):
    """
    Renders the actual DDL emitted by `create.sql` / `replace.sql`
    (src/dbt/include/snowflake/macros/relations/interactive_table/) end to end,
    using the real `optional()` macro and the real `SnowflakeInteractiveTableConfig`.

    This closes the gap that let a89a9a29's bug reach live testing: nothing
    previously rendered these two DDL macro files, so an `initialization_warehouse`
    clause emitted unconditionally (outside the `is_dynamic` gate that already
    protects `target_lag`/`warehouse`) was invisible to the unit suite even though
    Snowflake genuinely rejects it on a static table (001420).
    """

    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=[
                "jinja2.ext.do",
            ],
        )

        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "return": lambda r: r,
        }

        # `optional()` is called by bare name (not via adapter.dispatch), relying
        # on dbt-core's compiled macro namespace to make it resolvable at runtime.
        # Load the real macro and inject it as a context global so the DDL macros
        # exercise its actual none-handling behavior, not a stand-in.
        optional_template = self.jinja_env.get_template(
            "utils/optional.sql", globals=self.default_context
        )
        self.default_context["optional"] = optional_template.module.optional

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __create_mock_relation(
        self,
        interactive_table_config,
        database="test_db",
        schema="test_schema",
        identifier="test_table",
    ):
        mock_relation = mock.Mock()
        rendered = f"{database}.{schema}.{identifier}"
        mock_relation.__str__ = lambda self, _rendered=rendered: _rendered
        mock_relation.render.return_value = rendered
        mock_relation.from_config = mock.Mock(return_value=interactive_table_config)
        return mock_relation

    def __run(self, template_filename, macro_name, interactive_table_config, sql="select 1"):
        template = self.__get_template(template_filename)
        relation = self.__create_mock_relation(interactive_table_config)
        rendered = getattr(template.module, macro_name)(relation, sql)
        return re.sub(r"\s+", " ", rendered.strip())

    # --- case 1: static + snowflake_initialization_warehouse set (the bug) ------

    def test_create_static_with_initialization_warehouse_omits_all_dynamic_clauses(self):
        """The exact bug scenario: a project-wide `snowflake_initialization_warehouse`
        default lands on a static (no target_lag) table. Used to fail live with
        001420 because `initialization_warehouse` was emitted unconditionally."""
        config = SnowflakeInteractiveTableConfig(
            cluster_by="id",
            target_lag=None,
            snowflake_initialization_warehouse="INIT_WH",
        )
        self.assertFalse(config.is_dynamic)

        ddl = self.__run(
            "relations/interactive_table/create.sql",
            "snowflake__get_create_interactive_table_as_sql",
            config,
        )

        self.assertIn("create interactive table test_db.test_schema.test_table", ddl)
        self.assertIn("cluster by (id)", ddl)
        self.assertNotIn("initialization_warehouse", ddl)
        self.assertNotIn("target_lag", ddl)
        self.assertNotRegex(ddl, r"\bwarehouse\s*=")

    def test_replace_static_with_initialization_warehouse_omits_all_dynamic_clauses(self):
        config = SnowflakeInteractiveTableConfig(
            cluster_by="id",
            target_lag=None,
            snowflake_initialization_warehouse="INIT_WH",
        )
        self.assertFalse(config.is_dynamic)

        ddl = self.__run(
            "relations/interactive_table/replace.sql",
            "snowflake__get_replace_interactive_table_sql",
            config,
        )

        self.assertIn("create or replace interactive table test_db.test_schema.test_table", ddl)
        self.assertIn("cluster by (id)", ddl)
        self.assertNotIn("initialization_warehouse", ddl)
        self.assertNotIn("target_lag", ddl)
        self.assertNotRegex(ddl, r"\bwarehouse\s*=")

    # --- case 2: static + snowflake_warehouse set, no target_lag (companion) ----

    def test_create_static_with_snowflake_warehouse_omits_target_lag_and_warehouse(self):
        """Companion regression test for the target_lag/warehouse gate fixed
        earlier (2a01378b) -- confirms it's still correct."""
        config = SnowflakeInteractiveTableConfig(
            cluster_by="id",
            target_lag=None,
            snowflake_warehouse="ANALYTICS_WH",
        )
        self.assertFalse(config.is_dynamic)

        ddl = self.__run(
            "relations/interactive_table/create.sql",
            "snowflake__get_create_interactive_table_as_sql",
            config,
        )

        self.assertIn("create interactive table test_db.test_schema.test_table", ddl)
        self.assertIn("cluster by (id)", ddl)
        self.assertNotIn("target_lag", ddl)
        self.assertNotRegex(ddl, r"\bwarehouse\s*=")

    def test_replace_static_with_snowflake_warehouse_omits_target_lag_and_warehouse(self):
        config = SnowflakeInteractiveTableConfig(
            cluster_by="id",
            target_lag=None,
            snowflake_warehouse="ANALYTICS_WH",
        )
        self.assertFalse(config.is_dynamic)

        ddl = self.__run(
            "relations/interactive_table/replace.sql",
            "snowflake__get_replace_interactive_table_sql",
            config,
        )

        self.assertIn("create or replace interactive table test_db.test_schema.test_table", ddl)
        self.assertIn("cluster by (id)", ddl)
        self.assertNotIn("target_lag", ddl)
        self.assertNotRegex(ddl, r"\bwarehouse\s*=")

    # --- case 3: dynamic, no init warehouse (normal dynamic path) ---------------

    def test_create_dynamic_without_initialization_warehouse_emits_target_lag_and_warehouse(self):
        config = SnowflakeInteractiveTableConfig(
            cluster_by="id",
            target_lag="1 hour",
            snowflake_warehouse="ANALYTICS_WH",
        )
        self.assertTrue(config.is_dynamic)

        ddl = self.__run(
            "relations/interactive_table/create.sql",
            "snowflake__get_create_interactive_table_as_sql",
            config,
        )

        self.assertIn("target_lag = '1 hour'", ddl)
        self.assertIn("warehouse = ANALYTICS_WH", ddl)
        self.assertNotIn("initialization_warehouse", ddl)

    def test_replace_dynamic_without_initialization_warehouse_emits_target_lag_and_warehouse(self):
        config = SnowflakeInteractiveTableConfig(
            cluster_by="id",
            target_lag="1 hour",
            snowflake_warehouse="ANALYTICS_WH",
        )
        self.assertTrue(config.is_dynamic)

        ddl = self.__run(
            "relations/interactive_table/replace.sql",
            "snowflake__get_replace_interactive_table_sql",
            config,
        )

        self.assertIn("target_lag = '1 hour'", ddl)
        self.assertIn("warehouse = ANALYTICS_WH", ddl)
        self.assertNotIn("initialization_warehouse", ddl)

    # --- case 4: dynamic with all three fields set -------------------------------

    def test_create_dynamic_with_all_fields_emits_all_three_clauses(self):
        config = SnowflakeInteractiveTableConfig(
            cluster_by="id",
            target_lag="1 hour",
            refresh_warehouse="ANALYTICS_WH",
            snowflake_initialization_warehouse="INIT_WH",
        )
        self.assertTrue(config.is_dynamic)

        ddl = self.__run(
            "relations/interactive_table/create.sql",
            "snowflake__get_create_interactive_table_as_sql",
            config,
        )

        self.assertIn("target_lag = '1 hour'", ddl)
        self.assertIn("warehouse = ANALYTICS_WH", ddl)
        self.assertIn("initialization_warehouse = INIT_WH", ddl)

    def test_replace_dynamic_with_all_fields_emits_all_three_clauses(self):
        config = SnowflakeInteractiveTableConfig(
            cluster_by="id",
            target_lag="1 hour",
            refresh_warehouse="ANALYTICS_WH",
            snowflake_initialization_warehouse="INIT_WH",
        )
        self.assertTrue(config.is_dynamic)

        ddl = self.__run(
            "relations/interactive_table/replace.sql",
            "snowflake__get_replace_interactive_table_sql",
            config,
        )

        self.assertIn("target_lag = '1 hour'", ddl)
        self.assertIn("warehouse = ANALYTICS_WH", ddl)
        self.assertIn("initialization_warehouse = INIT_WH", ddl)


if __name__ == "__main__":
    unittest.main()
