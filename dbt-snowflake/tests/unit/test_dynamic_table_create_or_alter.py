import os
import re
import unittest
from unittest import mock

import snowflake.connector.errors
from jinja2 import Environment, FileSystemLoader

from dbt.adapters.snowflake.connections import (
    CREATE_OR_ALTER_FAILURE_HINT,
    CREATE_OR_ALTER_TRANSIENT_HINT,
    SnowflakeConnectionManager,
    _create_or_alter_hint,
)
from dbt_common.exceptions import DbtDatabaseError

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)

CREATE_OR_ALTER_TEMPLATE = "relations/dynamic_table/create_or_alter.sql"
CREATE_TEMPLATE = "relations/dynamic_table/create.sql"
REPLACE_TEMPLATE = "relations/dynamic_table/replace.sql"

MODEL_SQL = "select id,\n       value\nfrom source_table"


def _dynamic_table(**overrides):
    """A stand-in for SnowflakeDynamicTableConfig with the attributes the DDL macros read."""
    values = {
        "transient": None,
        "target_lag": "1 hour",
        "warehouse_parameter": "wh",
        "snowflake_initialization_warehouse": None,
        "refresh_mode": None,
        "initialize": None,
        "scheduler": None,
        "row_access_policy": None,
        "table_tag": None,
        "cluster_by": None,
        "immutable_where": None,
    }
    values.update(overrides)
    return mock.Mock(**values)


def _catalog_relation(catalog_type="INFO_SCHEMA"):
    return mock.Mock(
        catalog_type=catalog_type,
        external_volume="my_volume" if catalog_type == "BUILT_IN" else None,
        base_location=None,
        iceberg_version=None,
    )


class _MacroRenderer:
    """Renders dynamic table macros with a mocked dbt jinja context."""

    def __init__(self, context=None):
        self.env = Environment(loader=FileSystemLoader(MACROS_DIR), extensions=["jinja2.ext.do"])
        self.context = {
            "adapter": mock.Mock(),
            "config": mock.Mock(),
            "exceptions": mock.Mock(),
            "return": lambda value: value,
        }
        self.context["adapter"].behavior.snowflake_default_transient_dynamic_tables.no_warn = False
        self.context["adapter"].build_catalog_relation.return_value = _catalog_relation()
        self.context["optional"] = self.env.get_template("utils/optional.sql").module.optional
        self.context.update(context or {})

    def module(self, template_name):
        return self.env.get_template(template_name, globals=self.context).module


COA_SQL = "create or alter dynamic table t as (select 1)"
TRANSIENT_FLIP = (
    "001521 (55000): SQL compilation error: Property 'TRANSIENT_DATA' cannot be changed in "
    "CREATE OR ALTER."
)


class TestCreateOrAlterFailureHint(unittest.TestCase):
    def test_create_or_alter_error_gets_the_full_refresh_hint(self):
        self.assertEqual(
            _create_or_alter_hint("002108 (22000): CREATE OR ALTER execution failed.", COA_SQL),
            CREATE_OR_ALTER_FAILURE_HINT,
        )

    def test_ignores_leading_comments_and_case(self):
        sql = '/* {"app": "dbt"} */\n-- note\nCREATE  OR ALTER dynamic table t as (select 1)'
        self.assertEqual(
            _create_or_alter_hint("CREATE OR ALTER rejected", sql), CREATE_OR_ALTER_FAILURE_HINT
        )

    def test_transient_error_gets_the_transient_hint(self):
        hint = _create_or_alter_hint(TRANSIENT_FLIP, COA_SQL)
        self.assertEqual(hint, CREATE_OR_ALTER_TRANSIENT_HINT)
        self.assertIn("set `transient` explicitly", hint)
        self.assertIn("--full-refresh", hint)

    def test_errors_unrelated_to_create_or_alter_get_no_hint(self):
        self.assertIsNone(_create_or_alter_hint("003001 (42501): Insufficient privileges", COA_SQL))

    def test_other_statements_get_no_hint(self):
        for sql in (
            "create or replace dynamic table t as (select 1)",
            "alter dynamic table t set target_lag = '1 hour'",
            None,
        ):
            with self.subTest(sql=sql):
                self.assertIsNone(_create_or_alter_hint("CREATE OR ALTER rejected", sql))

    def test_exception_handler_appends_the_hint(self):
        error = snowflake.connector.errors.ProgrammingError(
            msg=TRANSIENT_FLIP, errno=1521, sqlstate="55000", sfqid="query-id"
        )
        with self.assertRaises(DbtDatabaseError) as raised:
            with SnowflakeConnectionManager.exception_handler(mock.Mock(), COA_SQL):
                raise error
        self.assertIn("Property 'TRANSIENT_DATA' cannot be changed", raised.exception.msg)
        self.assertIn(CREATE_OR_ALTER_TRANSIENT_HINT, raised.exception.msg)


class TestCreateOrAlterDdl(unittest.TestCase):
    def _render_info_schema(self, dynamic_table, sql=MODEL_SQL):
        renderer = _MacroRenderer()
        module = renderer.module(CREATE_OR_ALTER_TEMPLATE)
        return re.sub(
            r"\s+",
            " ",
            module.snowflake__create_or_alter_dynamic_table_info_schema_sql(
                dynamic_table, "db.sch.dt", sql
            ).strip(),
        )

    def test_info_schema_ddl_declares_full_state_without_create_only_clauses(self):
        sql = self._render_info_schema(
            _dynamic_table(
                refresh_mode="INCREMENTAL",
                cluster_by="id",
                initialize="ON_CREATE",
                row_access_policy="policy_name",
                table_tag="tag_name",
            )
        )
        self.assertTrue(sql.startswith("create or alter dynamic table db.sch.dt"))
        self.assertIn("warehouse = wh", sql)
        self.assertIn("refresh_mode = INCREMENTAL", sql)
        self.assertIn("cluster by (id)", sql)
        self.assertNotIn("initialize", sql)
        self.assertNotIn("row access policy", sql)
        self.assertNotIn("with tag", sql)
        self.assertNotIn("copy grants", sql)

    def test_info_schema_ddl_uses_transient_default_flag_when_unset(self):
        renderer = _MacroRenderer()
        renderer.context["adapter"].behavior.snowflake_default_transient_dynamic_tables.no_warn = (
            True
        )
        module = renderer.module(CREATE_OR_ALTER_TEMPLATE)
        sql = module.snowflake__create_or_alter_dynamic_table_info_schema_sql(
            _dynamic_table(transient=None), "db.sch.dt", MODEL_SQL
        )
        self.assertIn("create or alter transient dynamic table db.sch.dt", sql)

    def test_info_schema_ddl_explicit_transient_overrides_the_default_flag(self):
        renderer = _MacroRenderer()
        renderer.context["adapter"].behavior.snowflake_default_transient_dynamic_tables.no_warn = (
            True
        )
        module = renderer.module(CREATE_OR_ALTER_TEMPLATE)
        sql = module.snowflake__create_or_alter_dynamic_table_info_schema_sql(
            _dynamic_table(transient=False), "db.sch.dt", MODEL_SQL
        )
        self.assertIn("create or alter dynamic table db.sch.dt", sql)

    def test_info_schema_ddl_without_target_lag_declares_a_disabled_scheduler(self):
        sql = self._render_info_schema(_dynamic_table(target_lag=None))
        self.assertNotIn("target_lag", sql)
        self.assertIn("scheduler = 'DISABLE'", sql)

    def test_built_in_ddl_restates_iceberg_identity(self):
        renderer = _MacroRenderer()
        module = renderer.module(CREATE_OR_ALTER_TEMPLATE)
        sql = re.sub(
            r"\s+",
            " ",
            module.snowflake__create_or_alter_dynamic_table_built_in_sql(
                _dynamic_table(), "db.sch.dt", _catalog_relation("BUILT_IN"), MODEL_SQL
            ).strip(),
        )
        self.assertTrue(sql.startswith("create or alter dynamic iceberg table db.sch.dt"))
        self.assertIn("external_volume = 'my_volume'", sql)
        self.assertIn("catalog = 'SNOWFLAKE'", sql)
        self.assertNotIn("transient", sql)
        self.assertNotIn("initialize", sql)

    def test_query_block_is_byte_identical_to_create_and_replace(self):
        dynamic_table = _dynamic_table(cluster_by="id")
        renderer = _MacroRenderer()
        create = renderer.module(CREATE_TEMPLATE).snowflake__create_dynamic_table_info_schema_sql(
            dynamic_table, "db.sch.dt", MODEL_SQL
        )
        replace = renderer.module(
            REPLACE_TEMPLATE
        ).snowflake__replace_dynamic_table_info_schema_sql(dynamic_table, "db.sch.dt", MODEL_SQL)
        create_or_alter = renderer.module(
            CREATE_OR_ALTER_TEMPLATE
        ).snowflake__create_or_alter_dynamic_table_info_schema_sql(
            dynamic_table, "db.sch.dt", MODEL_SQL
        )

        def query_block(ddl):
            return ddl[ddl.index("as (") :].rstrip()

        self.assertEqual(query_block(create), query_block(replace))
        self.assertEqual(query_block(create), query_block(create_or_alter))
        self.assertIn("\n    as (\n        " + MODEL_SQL + "\n    )", create)

    def test_built_in_query_block_is_byte_identical_to_create(self):
        dynamic_table = _dynamic_table()
        catalog_relation = _catalog_relation("BUILT_IN")
        renderer = _MacroRenderer()
        create = renderer.module(CREATE_TEMPLATE).snowflake__create_dynamic_table_built_in_sql(
            dynamic_table, "db.sch.dt", MODEL_SQL
        )
        create_or_alter = renderer.module(
            CREATE_OR_ALTER_TEMPLATE
        ).snowflake__create_or_alter_dynamic_table_built_in_sql(
            dynamic_table, "db.sch.dt", catalog_relation, MODEL_SQL
        )
        self.assertEqual(
            create[create.index("as (") :].rstrip(),
            create_or_alter[create_or_alter.index("as (") :].rstrip(),
        )


class TestCreateOrAlterRouting(unittest.TestCase):
    """Walks the routing decisions of snowflake__get_create_or_alter_dynamic_table_sql."""

    def _route(
        self,
        *,
        existing_is_iceberg=False,
        catalog_type="INFO_SCHEMA",
        changes=None,
        on_configuration_change="apply",
    ):
        existing_relation = mock.Mock(is_iceberg_format=existing_is_iceberg)
        target_relation = mock.Mock()
        target_relation.from_config.return_value = _dynamic_table()

        config = mock.Mock(model=mock.Mock())
        config.get.side_effect = lambda key, default=None: (
            on_configuration_change if key == "on_configuration_change" else default
        )

        adapter = mock.Mock()
        adapter.behavior.snowflake_default_transient_dynamic_tables.no_warn = False
        adapter.build_catalog_relation.return_value = _catalog_relation(catalog_type)

        exceptions = mock.Mock()
        exceptions.warn.return_value = ""
        exceptions.raise_fail_fast_error.side_effect = RuntimeError("fail-fast")
        exceptions.raise_compiler_error.side_effect = ValueError("compiler-error")

        context = {
            "config": config,
            "adapter": adapter,
            "exceptions": exceptions,
            "snowflake__get_dynamic_table_configuration_changes": lambda existing, new: changes,
            "get_replace_sql": lambda existing, target, sql: "REPLACE(" + sql + ")",
        }
        renderer = _MacroRenderer(context)
        module = renderer.module(CREATE_OR_ALTER_TEMPLATE)
        out = module.snowflake__get_create_or_alter_dynamic_table_sql(
            existing_relation, target_relation, MODEL_SQL
        )
        return out.strip()

    def test_format_mismatch_rebuilds_with_replace(self):
        out = self._route(existing_is_iceberg=True, catalog_type="INFO_SCHEMA")
        self.assertTrue(out.startswith("REPLACE("))

    def test_native_to_iceberg_conversion_rebuilds_with_replace(self):
        out = self._route(existing_is_iceberg=False, catalog_type="BUILT_IN")
        self.assertTrue(out.startswith("REPLACE("))

    def test_query_only_edit_runs_create_or_alter(self):
        out = self._route(changes=None)
        self.assertTrue(out.startswith("create or alter dynamic table"))

    def test_full_refresh_change_rebuilds_with_replace(self):
        changes = mock.Mock(requires_full_refresh=True)
        out = self._route(changes=changes)
        self.assertTrue(out.startswith("REPLACE("))

    def test_in_place_change_runs_create_or_alter(self):
        changes = mock.Mock(requires_full_refresh=False)
        out = self._route(changes=changes)
        self.assertTrue(out.startswith("create or alter dynamic table"))

    def test_built_in_query_only_edit_runs_iceberg_create_or_alter(self):
        out = self._route(existing_is_iceberg=True, catalog_type="BUILT_IN", changes=None)
        self.assertTrue(out.startswith("create or alter dynamic iceberg table"))

    def test_continue_skips_and_fail_raises(self):
        changes = mock.Mock(requires_full_refresh=False)
        self.assertEqual(self._route(changes=changes, on_configuration_change="continue"), "")
        with self.assertRaises(RuntimeError):
            self._route(changes=changes, on_configuration_change="fail")

    def test_fail_does_not_block_a_query_only_edit(self):
        out = self._route(changes=None, on_configuration_change="fail")
        self.assertTrue(out.startswith("create or alter dynamic table"))

    def test_unknown_on_configuration_change_raises(self):
        changes = mock.Mock(requires_full_refresh=False)
        with self.assertRaises(ValueError):
            self._route(changes=changes, on_configuration_change="ignore")


class TestReplaceIcebergBaseLocation(unittest.TestCase):
    """REPLACE is now reached by native<->Iceberg and refresh_mode rebuilds, so it must omit an unset base_location."""

    def _replace_sql(self, base_location):
        catalog_relation = _catalog_relation("BUILT_IN")
        catalog_relation.base_location = base_location
        renderer = _MacroRenderer()
        renderer.context["adapter"].build_catalog_relation.return_value = catalog_relation
        module = renderer.module(REPLACE_TEMPLATE)
        return re.sub(
            r"\s+",
            " ",
            module.snowflake__replace_dynamic_table_built_in_sql(
                _dynamic_table(), "db.sch.dt", MODEL_SQL
            ).strip(),
        )

    def test_unset_base_location_is_omitted(self):
        sql = self._replace_sql(base_location=None)
        self.assertTrue(sql.startswith("create or replace dynamic iceberg table db.sch.dt"))
        self.assertNotIn("base_location", sql)
        self.assertNotIn("'None'", sql)

    def test_set_base_location_is_rendered_as_a_quoted_string(self):
        sql = self._replace_sql(base_location="my/path/")
        self.assertIn("base_location = 'my/path/'", sql)


if __name__ == "__main__":
    unittest.main()
