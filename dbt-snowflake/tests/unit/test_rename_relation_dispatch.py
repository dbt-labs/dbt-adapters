import os
import re
import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


class TestSnowflakeGetRenameSqlDispatch(unittest.TestCase):
    """
    Covers `snowflake__get_rename_sql` (src/dbt/include/snowflake/macros/relations/rename.sql),
    the dispatcher for interactive tables. `default__get_rename_sql` lives in
    dbt-adapters' global_project package and isn't resolvable via a FileSystemLoader scoped to
    dbt-snowflake's macros dir, so it's stubbed as a context global for the else-branch check.
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

        # Called by bare name from the dispatcher, which relies on dbt-core's compiled
        # macro namespace; inject the real macro rather than a stand-in.
        interactive_table_template = self.jinja_env.get_template(
            "relations/interactive_table/rename.sql", globals=self.default_context
        )
        self.default_context["snowflake__get_rename_interactive_table_sql"] = (
            interactive_table_template.module.snowflake__get_rename_interactive_table_sql
        )

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __create_mock_relation(
        self,
        is_interactive_table=False,
        database="test_db",
        schema="test_schema",
        identifier="test_table",
    ):
        mock_relation = mock.Mock()
        mock_relation.is_interactive_table = is_interactive_table
        rendered = f"{database}.{schema}.{identifier}"
        mock_relation.__str__ = lambda self, _rendered=rendered: _rendered
        mock_relation.render.return_value = rendered
        return mock_relation

    def test_macros_load(self):
        self.jinja_env.get_template("relations/rename.sql")

    def test_get_rename_sql_interactive_table(self):
        """is_interactive_table=True reaches snowflake__get_rename_interactive_table_sql,
        not the default__get_rename_sql fallback"""
        template = self.__get_template("relations/rename.sql")
        relation = self.__create_mock_relation(is_interactive_table=True)

        sql = template.module.snowflake__get_rename_sql(relation, "new_table")
        sql = re.sub(r"\s+", " ", sql.strip())

        # The macro embeds a `/* ... */` docstring comment before the DDL; assert on the
        # trailing DDL rather than requiring an exact full-string match against the comment.
        expected = "alter table test_db.test_schema.test_table rename to new_table"
        self.assertTrue(
            sql.endswith(expected),
            f"expected sql to end with {expected!r}, got {sql!r}",
        )
        self.assertNotIn("default dispatch", sql)

    def test_get_rename_sql_non_interactive_table_falls_back_to_default(self):
        """is_interactive_table=False falls through to default__get_rename_sql"""
        self.default_context["default__get_rename_sql"] = (
            lambda relation, new_name: f"-- default dispatch for {relation} -> {new_name}"
        )
        template = self.__get_template("relations/rename.sql")
        relation = self.__create_mock_relation(is_interactive_table=False)

        sql = template.module.snowflake__get_rename_sql(relation, "new_table")
        sql = re.sub(r"\s+", " ", sql.strip())

        expected = "-- default dispatch for test_db.test_schema.test_table -> new_table"
        self.assertEqual(sql, expected)


if __name__ == "__main__":
    unittest.main()
