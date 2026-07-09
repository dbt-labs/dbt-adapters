import os
import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


class _CompilerError(Exception):
    """Stand-in for dbt's compiler error so the test can assert on the raised message."""


class TestIcebergContractNumericPrecisionMacro(unittest.TestCase):
    """Unit tests for snowflake__get_table_columns_and_constraints (Iceberg bare-numeric guard)."""

    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do"],
        )
        self.exceptions = mock.Mock()

        def _raise(message):
            raise _CompilerError(message)

        self.exceptions.raise_compiler_error.side_effect = _raise

        self.context = {
            "exceptions": self.exceptions,
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            # base macro the override delegates to when the guard does not fire
            "table_columns_and_constraints": lambda: "COLUMNS_AND_CONSTRAINTS_DDL",
            "return": lambda value: value,
        }

    def _run(self, catalog_type, columns):
        context = dict(self.context)
        context["model"] = {"name": "m", "columns": columns}
        context["adapter"].build_catalog_relation = mock.Mock(
            return_value=mock.Mock(catalog_type=catalog_type)
        )
        template = self.jinja_env.get_template(
            "relations/column/columns_spec_ddl.sql", globals=context
        )
        return template.module.snowflake__get_table_columns_and_constraints()

    def test_iceberg_bare_number_raises_with_column_and_error_code(self):
        with self.assertRaises(_CompilerError) as ctx:
            self._run("BUILT_IN", {"id": {"name": "id", "data_type": "number"}})
        message = str(ctx.exception)
        self.assertIn("id", message)
        self.assertIn("099200", message)
        self.assertIn("number(38, 0)", message)

    def test_iceberg_rest_bare_decimal_raises(self):
        with self.assertRaises(_CompilerError):
            self._run("ICEBERG_REST", {"amt": {"name": "amt", "data_type": "DECIMAL"}})

    def test_iceberg_explicit_precision_does_not_raise(self):
        result = self._run("BUILT_IN", {"id": {"name": "id", "data_type": "number(38,0)"}})
        self.assertIn("COLUMNS_AND_CONSTRAINTS_DDL", result)

    def test_non_iceberg_bare_number_does_not_raise(self):
        result = self._run("INFO_SCHEMA", {"id": {"name": "id", "data_type": "number"}})
        self.assertIn("COLUMNS_AND_CONSTRAINTS_DDL", result)

    def test_iceberg_non_numeric_columns_do_not_raise(self):
        result = self._run(
            "BUILT_IN",
            {
                "id": {"name": "id", "data_type": "number(38,0)"},
                "name": {"name": "name", "data_type": "varchar"},
            },
        )
        self.assertIn("COLUMNS_AND_CONSTRAINTS_DDL", result)
