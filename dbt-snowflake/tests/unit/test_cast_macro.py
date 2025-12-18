import re
import unittest
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from unittest import mock


class TestSnowflakeCastMacro(unittest.TestCase):
    def setUp(self):
        snowflake_project_root = Path(__file__).resolve().parents[2]
        self.jinja_env = Environment(
            loader=FileSystemLoader(
                str(snowflake_project_root / "src/dbt/include/snowflake/macros")
            ),
            extensions=[
                "jinja2.ext.do",
            ],
        )

        self.default_context = {
            "adapter": mock.Mock(),
            "return": lambda r: r,
            # dbt provides a `modules` context object; for these unit tests we only need `re`.
            "modules": mock.Mock(re=re),
        }

    def __get_template(self, template_filename: str):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, macro_name: str, *args):
        value = getattr(template.module, macro_name)(*args)
        return re.sub(r"\s+", " ", value.strip())

    def test_cast_strips_collate_clause(self):
        template = self.__get_template("utils/cast.sql")
        sql = self.__run_macro(
            template, "snowflake__cast", "my_field", "VARCHAR(100) COLLATE 'en-ci'"
        )
        self.assertEqual(sql, "cast(my_field as VARCHAR(100))")

    def test_cast_strips_collation_clause(self):
        template = self.__get_template("utils/cast.sql")
        sql = self.__run_macro(
            template, "snowflake__cast", "my_field", 'VARCHAR(100) COLLATION "en-ci"'
        )
        self.assertEqual(sql, "cast(my_field as VARCHAR(100))")

    def test_cast_leaves_type_without_collation_untouched(self):
        template = self.__get_template("utils/cast.sql")
        sql = self.__run_macro(template, "snowflake__cast", "my_field", "NUMBER(38,0)")
        self.assertEqual(sql, "cast(my_field as NUMBER(38,0))")

    def test_cast_geography_uses_to_geography(self):
        template = self.__get_template("utils/cast.sql")
        sql = self.__run_macro(template, "snowflake__cast", "my_field", "GEOGRAPHY")
        self.assertEqual(sql, "to_geography(my_field)")

    def test_cast_geometry_uses_to_geometry(self):
        template = self.__get_template("utils/cast.sql")
        sql = self.__run_macro(template, "snowflake__cast", "my_field", "GEOMETRY")
        self.assertEqual(sql, "to_geometry(my_field)")
