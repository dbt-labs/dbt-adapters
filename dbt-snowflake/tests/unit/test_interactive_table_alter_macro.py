import os
import re
import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader

from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.adapters.snowflake.relation_configs.interactive_table import (
    SnowflakeInteractiveTableConfigChangeset,
    SnowflakeInteractiveTableInitializationWarehouseConfigChange,
    SnowflakeInteractiveTableRefreshWarehouseConfigChange,
    SnowflakeInteractiveTableTargetLagConfigChange,
)

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


class TestSnowflakeInteractiveTableAlterMacro(unittest.TestCase):
    """
    Renders the actual DDL emitted by `alter.sql`
    (src/dbt/include/snowflake/macros/relations/interactive_table/), using a real
    `SnowflakeInteractiveTableConfigChangeset` -- the same object type
    `SnowflakeRelation.interactive_table_config_changeset` builds -- rather than a
    mock standing in for it.
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
            "log": lambda msg, info=False: "",
            "return": lambda r: r,
        }

        # `snowflake__get_target_lag_warehouse_alter_sql()` is called by bare name from
        # `alter.sql`, relying on dbt-core's compiled macro namespace -- same pattern used for
        # `snowflake__interactive_table_ddl_body_sql` in
        # test_interactive_table_create_replace_macros.py. Load the real shared macro and
        # inject it as a context global so this test exercises the same code the runtime does.
        shared_alter_template = self.jinja_env.get_template(
            "relations/target_lag_warehouse_alter.sql", globals=self.default_context
        )
        self.default_context["snowflake__get_target_lag_warehouse_alter_sql"] = (
            shared_alter_template.module.snowflake__get_target_lag_warehouse_alter_sql
        )

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run(self, configuration_changes, existing_relation="test_db.test_schema.test_table"):
        template = self.__get_template("relations/interactive_table/alter.sql")
        rendered = template.module.snowflake__get_alter_interactive_table_as_sql(
            existing_relation,
            configuration_changes,
            "test_db.test_schema.test_table",
            "select 1",
        )
        return re.sub(r"\s+", " ", rendered.strip())

    # --- case 1: target_lag-only change ------------------------------------

    def test_target_lag_only_change_renders_single_set_statement(self):
        changes = SnowflakeInteractiveTableConfigChangeset(
            target_lag=SnowflakeInteractiveTableTargetLagConfigChange(
                action=RelationConfigChangeAction.alter,
                context="2 hours",
            )
        )

        ddl = self.__run(changes)

        self.assertIn("alter interactive table test_db.test_schema.test_table set", ddl)
        self.assertIn("target_lag = '2 hours'", ddl)
        self.assertNotIn(";", ddl)
        self.assertNotIn("unset", ddl)

    # --- case 2: initialization_warehouse cleared alone ---------------------

    def test_initialization_warehouse_cleared_alone_renders_bare_unset(self):
        changes = SnowflakeInteractiveTableConfigChangeset(
            snowflake_initialization_warehouse=(
                SnowflakeInteractiveTableInitializationWarehouseConfigChange(
                    action=RelationConfigChangeAction.alter,
                    context=None,
                )
            )
        )

        ddl = self.__run(changes)

        self.assertEqual(
            "alter interactive table test_db.test_schema.test_table unset "
            "initialization_warehouse",
            ddl,
        )
        self.assertNotIn(";", ddl)

    # --- case 3: target_lag changed AND initialization_warehouse cleared ----

    def test_target_lag_change_and_initialization_warehouse_clear_join_two_statements(self):
        changes = SnowflakeInteractiveTableConfigChangeset(
            target_lag=SnowflakeInteractiveTableTargetLagConfigChange(
                action=RelationConfigChangeAction.alter,
                context="2 hours",
            ),
            snowflake_initialization_warehouse=(
                SnowflakeInteractiveTableInitializationWarehouseConfigChange(
                    action=RelationConfigChangeAction.alter,
                    context=None,
                )
            ),
        )

        ddl = self.__run(changes)

        set_index = ddl.index("alter interactive table test_db.test_schema.test_table set")
        semicolon_index = ddl.index(";")
        unset_index = ddl.index(
            "alter interactive table test_db.test_schema.test_table unset "
            "initialization_warehouse"
        )

        self.assertIn("target_lag = '2 hours'", ddl)
        self.assertTrue(set_index < semicolon_index < unset_index)
        self.assertEqual(1, ddl.count(";"))

    # --- case 4: all three fields changed ------------------------------------

    def test_all_three_fields_changed_render_single_set_statement(self):
        changes = SnowflakeInteractiveTableConfigChangeset(
            target_lag=SnowflakeInteractiveTableTargetLagConfigChange(
                action=RelationConfigChangeAction.alter,
                context="2 hours",
            ),
            refresh_warehouse=SnowflakeInteractiveTableRefreshWarehouseConfigChange(
                action=RelationConfigChangeAction.alter,
                context="NEW_WH",
            ),
            snowflake_initialization_warehouse=(
                SnowflakeInteractiveTableInitializationWarehouseConfigChange(
                    action=RelationConfigChangeAction.alter,
                    context="INIT_WH",
                )
            ),
        )

        ddl = self.__run(changes)

        self.assertIn("alter interactive table test_db.test_schema.test_table set", ddl)
        self.assertIn("target_lag = '2 hours'", ddl)
        self.assertIn("warehouse = NEW_WH", ddl)
        self.assertIn("initialization_warehouse = INIT_WH", ddl)
        self.assertNotIn(";", ddl)
        self.assertNotIn("unset", ddl)


if __name__ == "__main__":
    unittest.main()
