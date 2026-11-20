import os
import re
import unittest
from types import SimpleNamespace
from unittest import mock

from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


def _mock_validate_doc_columns(relation, column_dict, existing_column_names):
    existing_lower = [c.lower() for c in existing_column_names]
    filtered = {}
    for col_name, col_meta in column_dict.items():
        if col_meta.get("quote"):
            present = col_name in existing_column_names
        else:
            present = col_name.lower() in existing_lower
        if present:
            filtered[col_name] = col_meta
    return filtered


class TestSnowflakeAlterColumnCommentMacro(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=[
                "jinja2.ext.do",
            ],
        )

        self.config = {}
        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "return": lambda r: r,
            "validate_doc_columns": _mock_validate_doc_columns,
        }
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )
        self.default_context["adapter"].quote = lambda col: f'"{col}"'

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, relation, column_dict, existing_columns):
        def dispatch(macro_name, macro_namespace=None, packages=None):
            return getattr(template.module, f"snowflake__{macro_name}")

        self.default_context["adapter"].dispatch = dispatch
        self.default_context["adapter"].get_columns_in_relation = lambda rel: [
            SimpleNamespace(name=c) for c in existing_columns
        ]

        value = getattr(template.module, "snowflake__alter_column_comment")(relation, column_dict)
        return re.sub(r"\s+", " ", value.strip())

    def __create_mock_relation(
        self,
        relation_type="table",
        is_dynamic_table=False,
        is_iceberg_format=False,
        is_interactive_table=False,
        database="test_db",
        schema="test_schema",
        identifier="test_table",
    ):
        mock_relation = mock.Mock()
        mock_relation.type = relation_type
        mock_relation.is_dynamic_table = is_dynamic_table
        mock_relation.is_iceberg_format = is_iceberg_format
        mock_relation.is_interactive_table = is_interactive_table
        mock_relation.get_ddl_prefix_for_alter = lambda: "iceberg" if is_iceberg_format else ""
        mock_relation.render.return_value = f"{database}.{schema}.{identifier}"
        return mock_relation

    def test_alter_column_comment_only_alters_documented_columns(self):
        """Ensure undocumented columns in the database are NOT altered or wiped out."""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table")
        existing_columns = ["ID", "NAME", "CREATED_AT", "STATUS"]
        column_dict = {
            "id": {"name": "id", "description": "Primary key", "quote": False},
        }

        sql = self.__run_macro(template, relation, column_dict, existing_columns)

        expected = 'alter table test_db.test_schema.test_table alter "ID" COMMENT $$Primary key$$;'
        self.assertEqual(sql, expected)
        self.assertNotIn("NAME", sql)
        self.assertNotIn("CREATED_AT", sql)
        self.assertNotIn("STATUS", sql)

    def test_alter_column_comment_multiple_columns(self):
        """Ensure multiple documented columns are emitted comma-separated ending with semicolon."""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table")
        existing_columns = ["ID", "NAME", "STATUS"]
        column_dict = {
            "id": {"name": "id", "description": "Primary key", "quote": False},
            "status": {"name": "status", "description": "Active status", "quote": False},
        }

        sql = self.__run_macro(template, relation, column_dict, existing_columns)

        expected = 'alter table test_db.test_schema.test_table alter "ID" COMMENT $$Primary key$$, "STATUS" COMMENT $$Active status$$;'
        self.assertEqual(sql, expected)
        self.assertNotIn("NAME", sql)

    def test_alter_column_comment_case_insensitive_matching(self):
        """Ensure column names match case-insensitively for unquoted identifiers."""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table")
        existing_columns = ["CUSTOMER_NAME"]
        column_dict = {
            "customer_name": {"name": "customer_name", "description": "Customer full name", "quote": False},
        }

        sql = self.__run_macro(template, relation, column_dict, existing_columns)

        expected = 'alter table test_db.test_schema.test_table alter "CUSTOMER_NAME" COMMENT $$Customer full name$$;'
        self.assertEqual(sql, expected)

    def test_alter_column_comment_quoted_column(self):
        """Ensure quoted columns match case-sensitively."""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table")
        existing_columns = ["custom_case", "OTHER_COL"]
        column_dict = {
            "custom_case": {"name": "custom_case", "description": "Exact case", "quote": True},
        }

        sql = self.__run_macro(template, relation, column_dict, existing_columns)

        expected = 'alter table test_db.test_schema.test_table alter "custom_case" COMMENT $$Exact case$$;'
        self.assertEqual(sql, expected)
        self.assertNotIn("OTHER_COL", sql)

    def test_alter_column_comment_dollar_sign_escaping(self):
        """Ensure dollar signs in descriptions are escaped as [$]."""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table")
        existing_columns = ["PRICE"]
        column_dict = {
            "price": {"name": "price", "description": "Price in $USD and $CAD", "quote": False},
        }

        sql = self.__run_macro(template, relation, column_dict, existing_columns)

        expected = 'alter table test_db.test_schema.test_table alter "PRICE" COMMENT $$Price in [$]USD and [$]CAD$$;'
        self.assertEqual(sql, expected)

    def test_alter_column_comment_iceberg_table(self):
        """Ensure iceberg tables include the iceberg prefix."""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table", is_iceberg_format=True)
        existing_columns = ["ID"]
        column_dict = {
            "id": {"name": "id", "description": "Iceberg PK", "quote": False},
        }

        sql = self.__run_macro(template, relation, column_dict, existing_columns)

        expected = 'alter iceberg table test_db.test_schema.test_table alter "ID" COMMENT $$Iceberg PK$$;'
        self.assertEqual(sql, expected)

    def test_alter_column_comment_dynamic_table(self):
        """Ensure dynamic tables render table as relation_type."""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="dynamic_table", is_dynamic_table=True)
        existing_columns = ["ID"]
        column_dict = {
            "id": {"name": "id", "description": "DT col", "quote": False},
        }

        sql = self.__run_macro(template, relation, column_dict, existing_columns)

        expected = 'alter table test_db.test_schema.test_table alter "ID" COMMENT $$DT col$$;'
        self.assertEqual(sql, expected)

    def test_alter_column_comment_interactive_table(self):
        """Ensure interactive tables render table as relation_type."""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="interactive_table", is_interactive_table=True)
        existing_columns = ["ID"]
        column_dict = {
            "id": {"name": "id", "description": "IT col", "quote": False},
        }

        sql = self.__run_macro(template, relation, column_dict, existing_columns)

        expected = 'alter table test_db.test_schema.test_table alter "ID" COMMENT $$IT col$$;'
        self.assertEqual(sql, expected)

    def test_alter_column_comment_no_matching_columns_emits_empty(self):
        """Ensure when no documented columns match the relation, no invalid ALTER statement is emitted."""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table")
        existing_columns = ["ID", "NAME"]
        column_dict = {
            "non_existent": {"name": "non_existent", "description": "Missing", "quote": False},
        }

        sql = self.__run_macro(template, relation, column_dict, existing_columns)

        self.assertEqual(sql, "")
