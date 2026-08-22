"""Regression coverage for dbt-labs/dbt-adapters#2106.

Kept separate from test_snowflake_adapter.py so it does not require dbt-core
(FileHash / config_from_parts) to exercise the SHOW USER FUNCTIONS gate.
"""

from types import SimpleNamespace
from unittest import TestCase
from unittest import mock

import agate

from dbt.adapters.snowflake import SnowflakeAdapter
from dbt.adapters.sql.impl import (
    LIST_FUNCTION_RELATIONS_MACRO_NAME,
    LIST_RELATIONS_MACRO_NAME,
)


class TestSnowflakeListFunctionRelationsGating(TestCase):
    def _adapter(self):
        adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
        adapter.config = SimpleNamespace(flags={})
        adapter._list_function_relations = False
        adapter.Relation = SnowflakeAdapter.Relation
        return adapter

    def _objects_table(self):
        return agate.Table(
            [("TEST_DATABASE", "PUBLIC", "MY_TABLE", "TABLE", "N", "N")],
            column_names=[
                "database_name",
                "schema_name",
                "name",
                "kind",
                "is_dynamic",
                "is_iceberg",
            ],
        )

    def _functions_table(self):
        # Keep is_builtin as Text ("N"/"Y") to match SHOW USER FUNCTIONS results.
        text = agate.Text()
        return agate.Table(
            [("TEST_DATABASE", "PUBLIC", "MY_UDF", "N")],
            column_names=["catalog_name", "schema_name", "name", "is_builtin"],
            column_types=[text, text, text, text],
        )

    def _schema_relation(self):
        return mock.Mock()

    def test_skips_show_user_functions_when_project_has_no_functions(self):
        adapter = self._adapter()
        with mock.patch.object(
            SnowflakeAdapter.__mro__[1], "set_relations_cache", return_value=None
        ):
            adapter.set_relations_cache([])

        self.assertFalse(adapter._list_function_relations)

        with mock.patch.object(
            adapter, "execute_macro", return_value=self._objects_table()
        ) as execute_macro:
            relations = adapter.list_relations_without_caching(self._schema_relation())

        self.assertEqual(len(relations), 1)
        self.assertEqual(relations[0].identifier, "MY_TABLE")
        self.assertEqual(execute_macro.call_count, 1)
        self.assertEqual(execute_macro.call_args.args[0], LIST_RELATIONS_MACRO_NAME)

    def test_lists_functions_when_project_has_function_nodes(self):
        adapter = self._adapter()
        with mock.patch.object(
            SnowflakeAdapter.__mro__[1], "set_relations_cache", return_value=None
        ):
            adapter.set_relations_cache([SimpleNamespace(resource_type="function")])

        self.assertTrue(adapter._list_function_relations)

        objects = self._objects_table()
        functions = self._functions_table()

        def _execute_macro(name, kwargs=None):
            if name == LIST_RELATIONS_MACRO_NAME:
                return objects
            if name == LIST_FUNCTION_RELATIONS_MACRO_NAME:
                return functions
            raise AssertionError(f"unexpected macro {name}")

        with mock.patch.object(adapter, "execute_macro", side_effect=_execute_macro) as execute_macro:
            relations = adapter.list_relations_without_caching(self._schema_relation())

        self.assertEqual({r.identifier for r in relations}, {"MY_TABLE", "MY_UDF"})
        self.assertEqual(execute_macro.call_count, 2)
        called = [c.args[0] for c in execute_macro.call_args_list]
        self.assertIn(LIST_FUNCTION_RELATIONS_MACRO_NAME, called)

    def test_flag_can_force_disable_function_listing(self):
        adapter = self._adapter()
        adapter.config.flags = {"list_function_relations": False}

        with mock.patch.object(
            SnowflakeAdapter.__mro__[1], "set_relations_cache", return_value=None
        ):
            adapter.set_relations_cache([SimpleNamespace(resource_type="function")])

        self.assertFalse(adapter._list_function_relations)

        with mock.patch.object(
            adapter, "execute_macro", return_value=self._objects_table()
        ) as execute_macro:
            relations = adapter.list_relations_without_caching(self._schema_relation())

        self.assertEqual(len(relations), 1)
        self.assertEqual(execute_macro.call_count, 1)

    def test_flag_can_force_enable_function_listing(self):
        adapter = self._adapter()
        adapter.config.flags = {"list_function_relations": True}

        with mock.patch.object(
            SnowflakeAdapter.__mro__[1], "set_relations_cache", return_value=None
        ):
            adapter.set_relations_cache([SimpleNamespace(resource_type="model")])

        self.assertTrue(adapter._list_function_relations)
        self.assertTrue(adapter._should_list_function_relations())
