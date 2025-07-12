import unittest
from unittest import mock
import re
from jinja2 import Environment, FileSystemLoader


class TestSnowflakeAlterRelationCommentMacro(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader("src/dbt/include/snowflake/macros"),
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
        }
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, name, relation, relation_comment):
        def dispatch(macro_name, macro_namespace=None, packages=None):
            return getattr(template.module, f"snowflake__{macro_name}")

        self.default_context["adapter"].dispatch = dispatch

        value = getattr(template.module, name)(relation, relation_comment)
        return re.sub(r"\s+", " ", value.strip())

    def __create_mock_relation(
        self,
        relation_type="table",
        is_dynamic_table=False,
        is_iceberg_format=False,
        database="test_db",
        schema="test_schema",
        identifier="test_table",
    ):
        """Create a mock relation object with the specified properties"""
        mock_relation = mock.Mock()
        mock_relation.type = relation_type
        mock_relation.is_dynamic_table = is_dynamic_table
        mock_relation.is_iceberg_format = is_iceberg_format
        mock_relation.render.return_value = f"{database}.{schema}.{identifier}"
        return mock_relation

    def test_macros_load(self):
        """Test that the adapters.sql template loads without errors"""
        self.jinja_env.get_template("adapters.sql")

    def test_alter_relation_comment_regular_table(self):
        """Test alter_relation_comment for a regular table"""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table")
        comment = "This is a test comment"

        sql = self.__run_macro(template, "snowflake__alter_relation_comment", relation, comment)

        expected = "comment on table test_db.test_schema.test_table IS $$This is a test comment$$;"
        self.assertEqual(sql, expected)

    def test_alter_relation_comment_regular_view(self):
        """Test alter_relation_comment for a regular view"""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="view")
        comment = "This is a view comment"

        sql = self.__run_macro(template, "snowflake__alter_relation_comment", relation, comment)

        expected = "comment on view test_db.test_schema.test_table IS $$This is a view comment$$;"
        self.assertEqual(sql, expected)

    def test_alter_relation_comment_dynamic_table(self):
        """Test alter_relation_comment for a dynamic table"""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table", is_dynamic_table=True)
        comment = "This is a dynamic table comment"

        sql = self.__run_macro(template, "snowflake__alter_relation_comment", relation, comment)

        expected = "comment on dynamic table test_db.test_schema.test_table IS $$This is a dynamic table comment$$;"
        self.assertEqual(sql, expected)

    def test_alter_relation_comment_iceberg_table(self):
        """Test alter_relation_comment for an iceberg table"""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table", is_iceberg_format=True)
        comment = "This is an iceberg table comment"

        sql = self.__run_macro(template, "snowflake__alter_relation_comment", relation, comment)

        expected = "alter iceberg table test_db.test_schema.test_table set comment = $$This is an iceberg table comment$$;"
        self.assertEqual(sql, expected)

    def test_alter_relation_comment_dollar_sign_escaping(self):
        """Test that dollar signs in comments are properly escaped"""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table")
        comment = "This comment has a $dollar sign and $multiple $signs"

        sql = self.__run_macro(template, "snowflake__alter_relation_comment", relation, comment)

        expected = "comment on table test_db.test_schema.test_table IS $$This comment has a [$]dollar sign and [$]multiple [$]signs$$;"
        self.assertEqual(sql, expected)

    def test_alter_relation_comment_iceberg_table_dollar_sign_escaping(self):
        """Test that dollar signs in iceberg table comments are properly escaped"""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table", is_iceberg_format=True)
        comment = "Iceberg table with $dollar signs: $100 and $200"

        sql = self.__run_macro(template, "snowflake__alter_relation_comment", relation, comment)

        expected = "alter iceberg table test_db.test_schema.test_table set comment = $$Iceberg table with [$]dollar signs: [$]100 and [$]200$$;"
        self.assertEqual(sql, expected)

    def test_alter_relation_comment_empty_comment(self):
        """Test alter_relation_comment with an empty comment"""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table")
        comment = ""

        sql = self.__run_macro(template, "snowflake__alter_relation_comment", relation, comment)

        expected = "comment on table test_db.test_schema.test_table IS $$$$;"
        self.assertEqual(sql, expected)

    def test_alter_relation_comment_multiline_comment(self):
        """Test alter_relation_comment with a multiline comment"""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="view")
        comment = """This is a multiline
        comment with
        multiple lines"""

        sql = self.__run_macro(template, "snowflake__alter_relation_comment", relation, comment)

        # Note: The macro should preserve the multiline structure
        expected_comment = comment.replace("$", "[$]")
        expected = f"comment on view test_db.test_schema.test_table IS $${expected_comment}$$;"
        # Normalize whitespace for comparison since our __run_macro helper compresses whitespace
        self.assertTrue("multiline" in sql and "multiple lines" in sql)
        self.assertTrue(sql.startswith("comment on view"))
        self.assertTrue(sql.endswith("$$;"))

    def test_alter_relation_comment_special_characters(self):
        """Test alter_relation_comment with various special characters"""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(relation_type="table")
        comment = (
            "Comment with 'quotes', \"double quotes\", and other chars: @#%^&*()[]{}|\\;:,.<>?"
        )

        sql = self.__run_macro(template, "snowflake__alter_relation_comment", relation, comment)

        # Only dollar signs should be escaped, other characters should remain as-is
        expected = "comment on table test_db.test_schema.test_table IS $$Comment with 'quotes', \"double quotes\", and other chars: @#%^&*()[]{}|\\;:,.<>?$$;"
        self.assertEqual(sql, expected)

    def test_alter_relation_comment_iceberg_dynamic_precedence(self):
        """Test that iceberg format takes precedence over dynamic table"""
        template = self.__get_template("adapters.sql")
        relation = self.__create_mock_relation(
            relation_type="table", is_dynamic_table=True, is_iceberg_format=True
        )
        comment = "This is both dynamic and iceberg"

        sql = self.__run_macro(template, "snowflake__alter_relation_comment", relation, comment)

        # Should use iceberg syntax, not dynamic table syntax
        expected = "alter iceberg table test_db.test_schema.test_table set comment = $$This is both dynamic and iceberg$$;"
        self.assertEqual(sql, expected)
