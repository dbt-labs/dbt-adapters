from unittest.mock import Mock
import pytest
from jinja2 import Environment, FileSystemLoader


class TestSubquerySemicolonMacros:
    @pytest.fixture
    def jinja_env(self):
        macro_dirs = [
            "src/dbt/include/global_project/macros/utils",
            "src/dbt/include/global_project/macros/adapters",
            "src/dbt/include/global_project/macros/relations/table",
            "src/dbt/include/global_project/macros/materializations/tests",
        ]
        return Environment(
            loader=FileSystemLoader(macro_dirs),
            extensions=["jinja2.ext.do"],
        )

    @pytest.mark.parametrize(
        "raw_sql,expected",
        [
            ("select 1 as id;", "select 1 as id"),
            ("select 1 as id;  \n\t", "select 1 as id"),
            ("select 1 as id;;  \n", "select 1 as id"),
            ("select 1 as id", "select 1 as id"),
            ("select ';'", "select ';'"),
            ("select ';';  \n", "select ';'"),
            ("select 'hello; world' as msg;", "select 'hello; world' as msg"),
            ("select 1 as id -- comment", "select 1 as id -- comment"),
            ("", ""),
            ("   \n", "   \n"),
            (None, None),
            (123, 123),
        ],
    )
    def test_strip_trailing_semicolon(self, jinja_env, raw_sql, expected):
        captured = {}

        def _mock_return(val):
            captured["val"] = val
            return ""

        template = jinja_env.get_template("strip_trailing_semicolon.sql", globals={"return": _mock_return})
        template.module.default__strip_trailing_semicolon(raw_sql)
        assert captured.get("val") == expected

    def test_get_empty_subquery_sql_strips_terminal_semicolon(self, jinja_env):
        captured = {}

        def _mock_return(val):
            captured["val"] = val
            return ""

        strip_template = jinja_env.get_template("strip_trailing_semicolon.sql", globals={"return": _mock_return})

        def _strip_helper(sql):
            strip_template.module.default__strip_trailing_semicolon(sql)
            return captured.get("val")

        template = jinja_env.get_template(
            "columns.sql",
            globals={
                "strip_trailing_semicolon": _strip_helper,
                "return": _mock_return,
            },
        )

        rendered = template.module.default__get_empty_subquery_sql("select 1 as id, 'foo' as val; \n\t")
        assert "select 1 as id, 'foo' as val;" not in rendered
        assert "select 1 as id, 'foo' as val" in rendered
        assert ") as __dbt_sbq" in rendered

    def test_get_select_subquery_strips_terminal_semicolon(self, jinja_env):
        captured = {}

        def _mock_return(val):
            captured["val"] = val
            return ""

        strip_template = jinja_env.get_template("strip_trailing_semicolon.sql", globals={"return": _mock_return})

        def _strip_helper(sql):
            strip_template.module.default__strip_trailing_semicolon(sql)
            return captured.get("val")

        adapter = Mock()
        adapter.dispatch.return_value = lambda: "id, val"

        template = jinja_env.get_template(
            "create.sql",
            globals={
                "strip_trailing_semicolon": _strip_helper,
                "adapter": adapter,
                "return": _mock_return,
            },
        )

        rendered = template.module.default__get_select_subquery("select id, val from my_table; \n")
        assert "from my_table;" not in rendered
        assert "from my_table" in rendered
        assert ") as model_subq" in rendered

    def test_get_unit_test_sql_strips_terminal_semicolon(self, jinja_env):
        captured = {}

        def _mock_return(val):
            captured["val"] = val
            return ""

        strip_template = jinja_env.get_template("strip_trailing_semicolon.sql", globals={"return": _mock_return})

        def _strip_helper(sql):
            strip_template.module.default__strip_trailing_semicolon(sql)
            return captured.get("val")

        adapter = Mock()
        adapter.quote = lambda x: f'"{x}"'
        dbt_obj = Mock()
        dbt_obj.string_literal = lambda x: f"'{x}'"

        template = jinja_env.get_template(
            "helpers.sql",
            globals={
                "strip_trailing_semicolon": _strip_helper,
                "adapter": adapter,
                "dbt": dbt_obj,
                "return": _mock_return,
            },
        )

        rendered = template.module.default__get_unit_test_sql(
            "select 1 as id; \n",
            "select 1 as id; \n",
            ["id"],
        )
        assert "select 1 as id;" not in rendered
        assert "select 1 as id" in rendered
        assert "_dbt_internal_unit_test_actual" in rendered
        assert "_dbt_internal_unit_test_expected" in rendered

    def test_get_limit_sql_strips_terminal_semicolon(self, jinja_env):
        captured = {}

        def _mock_return(val):
            captured["val"] = val
            return ""

        strip_template = jinja_env.get_template("strip_trailing_semicolon.sql", globals={"return": _mock_return})

        def _strip_helper(sql):
            strip_template.module.default__strip_trailing_semicolon(sql)
            return captured.get("val")

        template = jinja_env.get_template(
            "show.sql",
            globals={
                "strip_trailing_semicolon": _strip_helper,
                "return": _mock_return,
            },
        )

        rendered = template.module.default__get_limit_sql("select 1 as id; \n", 10)
        assert "select 1 as id;" not in rendered
        assert "select 1 as id\n  limit 10" in rendered or "select 1 as id" in rendered
