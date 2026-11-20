import os
import re
import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


class RaisedCompilerError(Exception):
    pass


def _column(name, data_type="integer"):
    column = mock.Mock()
    column.name = name
    column.data_type = data_type
    column.is_string.return_value = False
    return column


def _relation(relation_type):
    relation = mock.Mock()
    relation.is_interactive_table = relation_type == "interactive_table"
    relation.is_dynamic_table = relation_type == "dynamic_table"
    relation.is_iceberg_format = False
    relation.type = relation_type
    relation.get_ddl_prefix_for_alter.return_value = ""
    relation.render.return_value = '"DB"."SCH"."T"'
    relation.__str__ = mock.Mock(return_value='"DB"."SCH"."T"')
    return relation


class TestSnowflakeAlterRelationAddRemoveColumns(unittest.TestCase):
    """Renders the DDL `snowflake__alter_relation_add_remove_columns` emits.

    Reachable for an interactive table via the `incremental` materialization's
    `process_schema_changes` when a model previously built as `interactive_table`
    is rebuilt as `incremental` with `on_schema_change: sync_all_columns`.
    """

    def setUp(self):
        self.run_query = mock.Mock()

        def raise_compiler_error(message):
            raise RaisedCompilerError(message)

        exceptions = mock.Mock()
        exceptions.raise_compiler_error.side_effect = raise_compiler_error

        adapter = mock.Mock()
        adapter.quote.side_effect = lambda name: f'"{name}"'

        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do"],
        )
        self.template = self.jinja_env.get_template(
            "adapters.sql",
            globals={
                "adapter": adapter,
                "exceptions": exceptions,
                "run_query": self.run_query,
                "config": mock.Mock(),
                "model": mock.Mock(),
                "log": lambda msg, info=False: "",
                "return": lambda r: r,
            },
        )

    def __run(self, relation_type, add_columns=None, remove_columns=None):
        self.template.module.snowflake__alter_relation_add_remove_columns(
            _relation(relation_type), add_columns or [], remove_columns or []
        )
        return [
            re.sub(r"\s+", " ", call.args[0].strip()) for call in self.run_query.call_args_list
        ]

    def test_interactive_table_add_column_alters_as_a_plain_table(self):
        statements = self.__run("interactive_table", add_columns=[_column("new_col")])

        self.assertEqual(1, len(statements))
        self.assertIn('alter table "DB"."SCH"."T" add column', statements[0])
        self.assertNotIn("interactive_table", statements[0])
        self.assertNotIn("interactive table", statements[0])

    def test_interactive_table_drop_column_is_rejected(self):
        with self.assertRaises(RaisedCompilerError) as raised:
            self.__run("interactive_table", remove_columns=[_column("old_col")])

        self.assertIn("Columns cannot be removed from an interactive table", str(raised.exception))
        self.run_query.assert_not_called()

    def test_interactive_table_add_and_remove_together_is_rejected_before_any_ddl(self):
        with self.assertRaises(RaisedCompilerError):
            self.__run(
                "interactive_table",
                add_columns=[_column("new_col")],
                remove_columns=[_column("old_col")],
            )

        self.run_query.assert_not_called()

    def test_dynamic_table_still_alters_as_a_dynamic_table(self):
        statements = self.__run("dynamic_table", add_columns=[_column("new_col")])

        self.assertIn('alter dynamic table "DB"."SCH"."T" add column', statements[0])

    def test_plain_table_drop_column_is_still_allowed(self):
        statements = self.__run("table", remove_columns=[_column("old_col")])

        self.assertEqual(1, len(statements))
        self.assertIn('alter table "DB"."SCH"."T" drop column', statements[0])


if __name__ == "__main__":
    unittest.main()
