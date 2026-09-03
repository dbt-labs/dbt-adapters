import os
import unittest
from unittest import mock
from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


class TestSnowflakeSafeCastMacro(unittest.TestCase):
    """
    A dict or list field with a nested None used to render through Python's
    own str(), which spells null as None, producing invalid syntax inside
    Snowflake's object/array constructor literals (#15530). These pin the
    fix at the macro level, without needing a live warehouse.
    """

    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=[
                "jinja2.ext.do",
            ],
        )
        self.default_context = {
            "adapter": mock.Mock(),
            "dbt": mock.Mock(
                string_literal=lambda v: f"'{v}'",
                escape_single_quotes=lambda v: v.replace("'", "\\'"),
            ),
            "return": lambda r: r,
        }

    def __get_template(self):
        return self.jinja_env.get_template("utils/safe_cast.sql", globals=self.default_context)

    def __safe_cast(self, template, field, type_):
        return getattr(template.module, "snowflake__safe_cast")(field, type_).strip()

    def test_nested_null_in_a_dict_field_renders_as_null_not_none(self):
        template = self.__get_template()
        field = {
            "ADEF-DEPT": {"system_attribute_id": "department", "value": "5"},
            "ADEF-CUSTOM": {"system_attribute_id": None, "value": "99"},
        }

        sql = self.__safe_cast(template, field, "OBJECT")

        self.assertNotIn("None", sql)
        self.assertIn("'system_attribute_id': null", sql)

    def test_nested_null_in_a_list_field_renders_as_null_not_none(self):
        template = self.__get_template()
        field = [{"a": None}, {"a": "b"}]

        sql = self.__safe_cast(template, field, "ARRAY")

        self.assertNotIn("None", sql)
        self.assertEqual(sql, "try_cast([{'a': null}, {'a': 'b'}] as ARRAY)")

    def test_a_string_value_in_a_dict_field_keeps_its_quoting(self):
        template = self.__get_template()
        field = {"note": "it's fine"}

        sql = self.__safe_cast(template, field, "OBJECT")

        self.assertEqual(sql, "try_cast({'note': 'it\\'s fine'} as OBJECT)")

    def test_a_plain_scalar_field_is_unaffected(self):
        template = self.__get_template()

        self.assertEqual(self.__safe_cast(template, 5, "NUMBER"), "try_cast('5' as NUMBER)")
        self.assertEqual(
            self.__safe_cast(template, "hello", "VARCHAR"), "try_cast(hello as VARCHAR)"
        )
